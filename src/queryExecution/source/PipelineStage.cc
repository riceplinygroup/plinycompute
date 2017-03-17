/*****************************************************************************
 *                                                                           *
 *  Copyright 2018 Rice University                                           *
 *                                                                           *
 *  Licensed under the Apache License, Version 2.0 (the "License");          *
 *  you may not use this file except in compliance with the License.         *
 *  You may obtain a copy of the License at                                  *
 *                                                                           *
 *      http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                           *
 *  Unless required by applicable law or agreed to in writing, software      *
 *  distributed under the License is distributed on an "AS IS" BASIS,        *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *  See the License for the specific language governing permissions and      *
 *  limitations under the License.                                           *
 *                                                                           *
 *****************************************************************************/
#ifndef PIPELINE_STAGE_CC
#define PIPELINE_STAGE_CC

//by Jia, Sept 2016

#include "AbstractAggregateComp.h"
#include "StorageAddData.h"
#include "ComputePlan.h"
#include "ScanUserSet.h"
#include "PDBDebug.h"
#include "PipelineStage.h"
#include "PageCircularBufferIterator.h"
#include "DataProxy.h"
#include "PageScanner.h"
#include "PageCircularBufferIterator.h"
#include "BlockQueryProcessor.h"
#include "InterfaceFunctions.h"
#include "HermesExecutionServer.h"
#include "GenericWork.h"
#include "SingleTableBundleProcessor.h"
#include "SetSpecifier.h"
#include "UseTemporaryAllocationBlock.h"
#include "Configuration.h"

namespace pdb {

PipelineStage :: ~PipelineStage () {
    this->jobStage = nullptr;
}

PipelineStage :: PipelineStage (Handle<TupleSetJobStage> stage, SharedMemPtr shm, PDBLoggerPtr logger, ConfigurationPtr conf, NodeID nodeId, size_t batchSize, int numThreads) {
    this->jobStage = stage;
    this->batchSize = batchSize;
    this->numThreads = numThreads;
    this->nodeId = nodeId;
    this->logger = logger;
    this->conf = conf;
    this->shm = shm;
    this->id = 0;
}


Handle<TupleSetJobStage> & PipelineStage :: getJobStage () {
    return jobStage;
}


int PipelineStage :: getNumThreads () {
    return this->numThreads;

}

//send repartitioned data to a remote node
bool PipelineStage :: storeShuffleData (Handle <Vector <Handle<Object>>> data, std :: string databaseName, std :: string setName, std :: string address, std :: string & errMsg) {
             return simpleSendDataRequest <StorageAddData, Handle <Object>, SimpleRequestResult, bool> (logger, conf->getPort(), address, false, 1024,
                 [&] (Handle <SimpleRequestResult> result) {
                     if (result != nullptr)
                         if (!result->getRes ().first) {
                             logger->error ("Error sending data: " + result->getRes ().second);
                             errMsg = "Error sending data: " + result->getRes ().second;
                         }
                         return true;}, data, databaseName, setName, "Shuffle", false);
        }


void PipelineStage :: runPipeline (HermesExecutionServer * server) {
    //std :: cout << "Pipeline network is running" << std :: endl;
    bool success;
    std :: string errMsg;

    //initialize the data proxy, scanner and set iterators
    PDBCommunicatorPtr communicatorToFrontend = make_shared<PDBCommunicator>(); 
    communicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);

    PDBLoggerPtr scannerLogger = make_shared<PDBLogger>("scanner.log");

    //getScanner
    int backendCircularBufferSize = 1;
    if (conf->getShmSize()/conf->getPageSize()-2 < 2+2*numThreads+backendCircularBufferSize) {
        success = false;
        errMsg = "Error: Not enough buffer pool size to run the query!";
        std :: cout << errMsg << std :: endl;
        return;
    }
    backendCircularBufferSize = (conf->getShmSize()/conf->getPageSize()-4-2*numThreads);
    if (backendCircularBufferSize > 10) {
       backendCircularBufferSize = 10;
    }

    PDB_COUT << "backendCircularBufferSize is tuned to be " << backendCircularBufferSize << std :: endl;

    PageScannerPtr scanner = make_shared<PageScanner>(communicatorToFrontend, shm, scannerLogger, numThreads, backendCircularBufferSize, nodeId); 
    if (server->getFunctionality<HermesExecutionServer>().setCurPageScanner(scanner) == false) {
        success = false;
        errMsg = "Error: A job is already running!";
        std :: cout << errMsg << std :: endl;
        return;
    }


    //get iterators
    //TODO: we should get iterators using only databaseName and setName
    PDB_COUT << "To send GetSetPages message" << std :: endl;
    std :: vector<PageCircularBufferIteratorPtr> iterators = scanner->getSetIterators(nodeId, jobStage->getSourceContext()->getDatabaseId(), jobStage->getSourceContext()->getTypeId(), jobStage->getSourceContext()->getSetId());
    PDB_COUT << "GetSetPages message is sent" << std :: endl;
    int numIteratorsReturned = iterators.size();
    if (numIteratorsReturned != numThreads) {
         success = false;
         errMsg = "Error: number of iterators doesn't match number of threads!";
         std :: cout << errMsg << std :: endl;
         return;
    }   

    //get output set information
    //now due to limitation in object model, we only support one output for a pipeline network
    SetSpecifierPtr outputSet = make_shared<SetSpecifier>(jobStage->getSinkContext()->getDatabase(), jobStage->getSinkContext()->getSetName(), jobStage->getSinkContext()->getDatabaseId(), jobStage->getSinkContext()->getTypeId(), jobStage->getSinkContext()->getSetId());
    
    pthread_mutex_t connection_mutex;
    pthread_mutex_init(&connection_mutex, nullptr);    

    //create a buzzer and counter
    PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>(
         [&] (PDBAlarm myAlarm, int & counter) {
             counter ++;
             //std :: cout << "counter = " << counter << std :: endl;
         });
    std :: cout << "to run pipeline with " << numThreads << " threads." << std :: endl;    
    int counter = 0;
    for (int i = 0; i < numThreads; i++) {
         PDBWorkerPtr worker = server->getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
         PDB_COUT << "to run the " << i << "-th work..." << std :: endl;
         //TODO: start threads
         PDBWorkPtr myWork = make_shared<GenericWork> (
             [&, i] (PDBBuzzerPtr callerBuzzer) {
                  //create a data proxy
                  std :: string loggerName = std :: string("PipelineStage_")+std :: to_string(i);
                  PDBLoggerPtr logger = make_shared<PDBLogger>(loggerName);
                  pthread_mutex_lock(&connection_mutex);
                  PDBCommunicatorPtr anotherCommunicatorToFrontend = make_shared<PDBCommunicator>();
                  anotherCommunicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);
                  pthread_mutex_unlock(&connection_mutex);
                  DataProxyPtr proxy = make_shared<DataProxy>(nodeId, anotherCommunicatorToFrontend, shm, logger);

                  //setup an output page to store intermediate results and final output
                  const UseTemporaryAllocationBlock tempBlock {4 * 1024 * 1024};
                  PDBPagePtr output=nullptr;
                  PageCircularBufferIteratorPtr iter = iterators.at(i);
                  std :: cout << i << ": to get compute plan" << std :: endl;
                  Handle<ComputePlan> plan = this->jobStage->getComputePlan();
                  plan->nullifyPlanPointer();
                  std :: cout << i << ": to deep copy ComputePlan object" << std :: endl;
                  Handle<ComputePlan> newPlan = deepCopyToCurrentAllocationBlock<ComputePlan>(plan);                  
                  std :: string sourceSpecifier = jobStage->getSourceTupleSetSpecifier();
                  std :: cout << "Source tupleset name=" << sourceSpecifier << std :: endl;
                  std :: string producerComputationName = newPlan->getProducingComputationName(sourceSpecifier);
                  std :: cout << "Producer computation name=" << producerComputationName << std :: endl;
                  Handle<Computation> computation = newPlan->getPlan()->getNode(producerComputationName).getComputationHandle();
                  Handle<ScanUserSet<Object>> scanner = unsafeCast<ScanUserSet<Object>, Computation>(computation);
                  scanner->setIterator(iter);
                  scanner->setProxy(proxy);
                  scanner->setBatchSize(batchSize);
                  newPlan->nullifyPlanPointer();
                  PipelinePtr curPipeline = newPlan->buildPipeline (
                  this->jobStage->getSourceTupleSetSpecifier(),
                  this->jobStage->getTargetTupleSetSpecifier(),
                  this->jobStage->getTargetComputationSpecifier(),
                  [] () -> std :: pair <void *, size_t> {
                      PDB_COUT << "to get a new page for writing" << std :: endl;
                      void * myPage = malloc (DEFAULT_NET_PAGE_SIZE);
                      return std :: make_pair(myPage, DEFAULT_NET_PAGE_SIZE);
                  },

                  [] (void * page) {
                      PDB_COUT << "to discard a page" << std :: endl;
                      free (page);
                  },

                  [&] (void * page) {
                      PDB_COUT << "to write back a page" << std :: endl;
                      PDBPagePtr output;
                      proxy->addUserPage(outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                      memcpy(output->getBytes(), page, DEFAULT_NET_PAGE_SIZE);
                      proxy->unpinUserPage(nodeId, output->getDbID(), output->getTypeID(), output->getSetID(), output);
                  }
);
                  std :: cout << "\nRunning Pipeline\n";
                  curPipeline->run();
                  curPipeline = nullptr;
                  this->jobStage->getComputePlan()->nullifyPlanPointer(); 
                  callerBuzzer->buzz(PDBAlarm :: WorkAllDone, counter);

             }

         );
         worker->execute(myWork, tempBuzzer);
    }

    while (counter < numThreads) {
         tempBuzzer->wait();
    }

    pthread_mutex_destroy(&connection_mutex);

    if (server->getFunctionality<HermesExecutionServer>().setCurPageScanner(nullptr) == false) {
        success = false;
        errMsg = "Error: No job is running!";
        std :: cout << errMsg << std :: endl;
        return;
    }

    return;

}

void PipelineStage :: runPipelineWithShuffleSink (HermesExecutionServer * server) {
    //std :: cout << "Pipeline network is running" << std :: endl;
    bool success;
    std :: string errMsg;

    int numNodes = jobStage->getNumNodes();
    size_t combinerPageSize = 128 * 1024 * 1024;
    //each queue has multiple producers and one consumer
    int combinerBufferSize = numThreads / numNodes + 1;
    std :: vector <PageCircularBufferPtr> combinerBuffers;
    std :: vector <PageCircularBufferIteratorPtr> combinerIters;

    pthread_mutex_t connection_mutex;
    pthread_mutex_init(&connection_mutex, nullptr);

    //create data proxy
    pthread_mutex_lock(&connection_mutex);
    PDBCommunicatorPtr anotherCommunicatorToFrontend = make_shared<PDBCommunicator>();
    anotherCommunicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);
    pthread_mutex_unlock(&connection_mutex);
    DataProxyPtr proxy = make_shared<DataProxy>(nodeId, anotherCommunicatorToFrontend, shm, logger);

    //create a buzzer and counter
    PDBBuzzerPtr combinerBuzzer = make_shared<PDBBuzzer>(
         [&] (PDBAlarm myAlarm, int & combinerCounter) {
             combinerCounter ++;
             PDB_COUT << "combinerCounter = " << combinerCounter << std :: endl;
         });
    std :: cout << "to run combiner with " << numNodes << " threads." << std :: endl;
    int combinerCounter = 0;

    int i;
    for ( i = 0; i < numNodes; i ++ ) {
        PageCircularBufferPtr buffer = make_shared<PageCircularBuffer>(combinerBufferSize, logger);
        combinerBuffers.push_back(buffer);
        PageCircularBufferIteratorPtr iter = make_shared<PageCircularBufferIterator> (i, buffer, logger);
        combinerIters.push_back(iter);
        PDBWorkerPtr worker = server->getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
        PDB_COUT << "to run the " << i << "-th work..." << std :: endl;
        // start threads
        PDBWorkPtr myWork = make_shared<GenericWork> (
             [&, i] (PDBBuzzerPtr callerBuzzer) {
                  std :: string errMsg;

                  //get the i-th address
                  std :: string address = this->jobStage->getIPAddress(i);

                  //get aggregate computation 
                  std :: cout << i << ": to get compute plan" << std :: endl;
                  const UseTemporaryAllocationBlock tempBlock {4 * 1024 * 1024};
                  Handle<ComputePlan> plan = this->jobStage->getComputePlan();
                  plan->nullifyPlanPointer();
                  std :: cout << i << ": to deep copy ComputePlan object" << std :: endl;
                  Handle<ComputePlan> newPlan = deepCopyToCurrentAllocationBlock<ComputePlan>(plan);
                  std :: string targetSpecifier = jobStage->getTargetComputationSpecifier();
                  std :: cout << "target computation name=" << targetSpecifier << std :: endl;
                  Handle<Computation> computation = newPlan->getPlan()->getNode(targetSpecifier).getComputationHandle();
                  Handle<AbstractAggregateComp> aggregate = unsafeCast<AbstractAggregateComp, Computation> (computation);
                  Handle<Vector<HashPartitionID>> nodePartitionIds = this->jobStage->getNumPartitions(i);

                  //get combiner processor
                  SimpleSingleTableQueryProcessorPtr combinerProcessor = 
                      aggregate->getCombinerProcessor(*nodePartitionIds);
                  
                  void * combinerPage = (void *) malloc (combinerPageSize * sizeof(char));
                  combinerProcessor->loadOutputPage(combinerPage, combinerPageSize);
                  while (iter->hasNext()) {
                      PDBPagePtr page = iter->next();
                      if (page != nullptr) {
                          //to load input page
                          combinerProcessor->loadInputPage(page->getBytes());
                          while (combinerProcessor->fillNextOutputPage()) {
                              //send out the output page
                              Record<Vector<Handle<Object>>> * record = (Record<Vector<Handle<Object>>> *)combinerPage;
                              this->storeShuffleData(record->getRootObject(), this->jobStage->getSinkContext()->getDatabase(), this->jobStage->getSinkContext()->getSetName(), address, errMsg); 
                              //free the output page
                              free(combinerPage);
                              //allocate a new page
                              combinerPage = (void *) malloc (combinerPageSize * sizeof(char));
                              //load the new page as output vector
                              combinerProcessor->loadOutputPage(combinerPage, combinerPageSize);

                          }
                          //unpin the input page
                          page->decRefCount();
                          if (page->getRefCount() == 0) {
                               proxy->unpinUserPage (nodeId, page->getDbID(), page->getTypeID(), page->getSetID(), page);
                          }        
                      }
                  }
                  combinerProcessor->finalize();
                  //send the output page
                  Record<Vector<Handle<Object>>> * record = (Record<Vector<Handle<Object>>> *)combinerPage;
                  this->storeShuffleData(record->getRootObject(), this->jobStage->getSinkContext()->getDatabase(), this->jobStage->getSinkContext()->getSetName(), address, errMsg);
                  //free the output page
                  free(combinerPage);
                  //allocate a new page
                  combinerPage = (void *) malloc (combinerPageSize * sizeof(char));
                  //load the new page as output vector
                  combinerProcessor->loadOutputPage(combinerPage, combinerPageSize);

                  callerBuzzer->buzz(PDBAlarm :: WorkAllDone, combinerCounter);
             }

         );
         worker->execute(myWork, combinerBuzzer);
    }


    //initialize the data proxy, scanner and set iterators
    PDBCommunicatorPtr communicatorToFrontend = make_shared<PDBCommunicator>();
    communicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);

    PDBLoggerPtr scannerLogger = make_shared<PDBLogger>("scanner.log");

    //getScanner
    int backendCircularBufferSize = 1;
    if (conf->getShmSize()/conf->getPageSize()-2 < 2+2*numThreads+backendCircularBufferSize) {
        success = false;
        errMsg = "Error: Not enough buffer pool size to run the query!";
        std :: cout << errMsg << std :: endl;
        return;
    }
    backendCircularBufferSize = (conf->getShmSize()/conf->getPageSize()-4-2*numThreads);
    if (backendCircularBufferSize > 10) {
       backendCircularBufferSize = 10;
    }

    PDB_COUT << "backendCircularBufferSize is tuned to be " << backendCircularBufferSize << std :: endl;

    PageScannerPtr scanner = make_shared<PageScanner>(communicatorToFrontend, shm, scannerLogger, numThreads, backendCircularBufferSize, nodeId);
    if (server->getFunctionality<HermesExecutionServer>().setCurPageScanner(scanner) == false) {
        success = false;
        errMsg = "Error: A job is already running!";
        std :: cout << errMsg << std :: endl;
        return;
    }

    //get iterators
    //TODO: we should get iterators using only databaseName and setName
    PDB_COUT << "To send GetSetPages message" << std :: endl;
    std :: vector<PageCircularBufferIteratorPtr> iterators = scanner->getSetIterators(nodeId, jobStage->getSourceContext()->getDatabaseId(), jobStage->getSourceContext()->getTypeId(), jobStage->getSourceContext()->getSetId());
    PDB_COUT << "GetSetPages message is sent" << std :: endl;
    int numIteratorsReturned = iterators.size();
    if (numIteratorsReturned != numThreads) {
         success = false;
         errMsg = "Error: number of iterators doesn't match number of threads!";
         std :: cout << errMsg << std :: endl;
         return;
    }


    //to run the pipeline threads
    //get output set information
    //now due to limitation in object model, we only support one output for a pipeline network
    SetSpecifierPtr outputSet = make_shared<SetSpecifier>(jobStage->getCombinerContext()->getDatabase(), jobStage->getCombinerContext()->getSetName(), jobStage->getCombinerContext()->getDatabaseId(), jobStage->getCombinerContext()->getTypeId(), jobStage->getCombinerContext()->getSetId());


    //create a buzzer and counter
    PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>(
         [&] (PDBAlarm myAlarm, int & counter) {
             counter ++;
             std :: cout << "counter = " << counter << std :: endl;
         });
    std :: cout << "to run pipeline with " << numThreads << " threads." << std :: endl;
    int counter = 0;
    for (int i = 0; i < numThreads; i++) {
         PDBWorkerPtr worker = server->getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
         PDB_COUT << "to run the " << i << "-th work..." << std :: endl;
         //TODO: start threads
         PDBWorkPtr myWork = make_shared<GenericWork> (
             [&, i] (PDBBuzzerPtr callerBuzzer) {
                  //create a data proxy
                  std :: string loggerName = std :: string("PipelineStage_")+std :: to_string(i);
                  PDBLoggerPtr logger = make_shared<PDBLogger>(loggerName);
                  pthread_mutex_lock(&connection_mutex);
                  PDBCommunicatorPtr anotherCommunicatorToFrontend = make_shared<PDBCommunicator>();
                  anotherCommunicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);
                  pthread_mutex_unlock(&connection_mutex);
                  DataProxyPtr proxy = make_shared<DataProxy>(nodeId, anotherCommunicatorToFrontend, shm, logger);

                  //setup an output page to store intermediate results and final output
                  const UseTemporaryAllocationBlock tempBlock {4 * 1024 * 1024};
                  PDBPagePtr output=nullptr;
                  PageCircularBufferIteratorPtr iter = iterators.at(i);
                  std :: cout << i << ": to get compute plan" << std :: endl;
                  Handle<ComputePlan> plan = this->jobStage->getComputePlan();
                  plan->nullifyPlanPointer();
                  std :: cout << i << ": to deep copy ComputePlan object" << std :: endl;
                  Handle<ComputePlan> newPlan = deepCopyToCurrentAllocationBlock<ComputePlan>(plan);
                  std :: string sourceSpecifier = jobStage->getSourceTupleSetSpecifier();
                  std :: cout << "Source tupleset name=" << sourceSpecifier << std :: endl;
                  std :: string producerComputationName = newPlan->getProducingComputationName(sourceSpecifier);
                  std :: cout << "Producer computation name=" << producerComputationName << std :: endl;
                  Handle<Computation> computation = newPlan->getPlan()->getNode(producerComputationName).getComputationHandle();
                  Handle<ScanUserSet<Object>> scanner = unsafeCast<ScanUserSet<Object>, Computation>(computation);
                  scanner->setIterator(iter);
                  scanner->setProxy(proxy);
                  scanner->setBatchSize(batchSize);
                  newPlan->nullifyPlanPointer();
                  PipelinePtr curPipeline = newPlan->buildPipeline (
                  this->jobStage->getSourceTupleSetSpecifier(),
                  this->jobStage->getTargetTupleSetSpecifier(),
                  this->jobStage->getTargetComputationSpecifier(),
                  [] () -> std :: pair <void *, size_t> {
                      PDB_COUT << "to get a new page for writing" << std :: endl;
                      void * myPage = malloc (DEFAULT_NET_PAGE_SIZE);
                      return std :: make_pair(myPage, DEFAULT_NET_PAGE_SIZE);
                  },

                  [] (void * page) {
                      PDB_COUT << "to discard a page" << std :: endl;
                      free (page);
                  },

                  [&] (void * page) {
                      PDB_COUT << "to write back a page" << std :: endl;
                      PDBPagePtr output;
                      proxy->addUserPage(outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                      memcpy(output->getBytes(), page, DEFAULT_NET_PAGE_SIZE);
                      int k;
                      for ( k = 0; k < numNodes; k ++ ) {
                          PageCircularBufferPtr buffer = combinerBuffers[k];
                          buffer->addPageToTail(output);
                          output->incRefCount();
                      } 
                  }
);
                  std :: cout << "\nRunning Pipeline\n";
                  curPipeline->run();
                  curPipeline = nullptr;
                  this->jobStage->getComputePlan()->nullifyPlanPointer();
                  callerBuzzer->buzz(PDBAlarm :: WorkAllDone, counter);

             }

         );
         worker->execute(myWork, tempBuzzer);
    }

    while (counter < numThreads) {
         tempBuzzer->wait();
    }
   
    int k;
    for ( k = 0; k < numNodes; k ++) {
         PageCircularBufferPtr buffer = combinerBuffers[k];
         buffer->close();
    }

    while (combinerCounter < numNodes) {
         combinerBuzzer->wait();
    }


    pthread_mutex_destroy(&connection_mutex);

    if (server->getFunctionality<HermesExecutionServer>().setCurPageScanner(nullptr) == false) {
        success = false;
        errMsg = "Error: No job is running!";
        std :: cout << errMsg << std :: endl;
        return;
    }

    return;
}

}

#endif
