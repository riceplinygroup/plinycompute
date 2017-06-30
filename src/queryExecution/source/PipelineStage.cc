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
#include "StorageAddObject.h"
#include "ComputePlan.h"
#include "ScanUserSet.h"
#include "SelectionComp.h"
#include "MultiSelectionComp.h"
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
#include "ClusterAggregateComp.h"
#include "SharedHashSet.h"
#include "JoinComp.h"
#include "SimpleSendObjectRequest.h"

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
bool PipelineStage :: storeShuffleData (Handle <Vector <Handle<Object>>> data, std :: string databaseName, std :: string setName, std :: string address, int port, std :: string & errMsg) {
       if(port <= 0) {
           port = conf->getPort();
       } 
       std :: cout << "store shuffle data to address=" << address << " and port=" << port << ", with size = " << data->size() << " to database=" << databaseName << " and set=" << setName << " and type = Aggregation" << std :: endl;
       return simpleSendDataRequest <StorageAddData, Handle <Object>, SimpleRequestResult, bool> (logger, port, address, false, 1024,
                 [&] (Handle <SimpleRequestResult> result) {
                     if (result != nullptr)
                         if (!result->getRes ().first) {
                             logger->error ("Error sending data: " + result->getRes ().second);
                             errMsg = "Error sending data: " + result->getRes ().second;
                         }
                         return true;}, data, databaseName, setName, "IntermediateData", false, false);
        }

//broadcast data
bool PipelineStage :: broadcastData (HermesExecutionServer * server, void * data, size_t size, std :: string databaseName, std :: string setName, std :: string &errMsg) {
    if (data == nullptr) {
        return false;
    }
    int numNodes = this->jobStage->getNumNodes();
    UseTemporaryAllocationBlock tempBlock {4*1024*1024};
    for (int i = 0; i < numNodes; i++) {
                  PDBCommunicator temp;
                  bool success;
                  std :: string errMsg;
                  std :: string address = this->jobStage->getIPAddress (i);
                  int port = this->jobStage->getPort (i);
                  PDB_COUT << "address=" << address << ", port=" << port << std :: endl;
                  temp.connectToInternetServer (logger, port, address, errMsg);
                  Handle<StorageAddObject> request = makeObject <StorageAddObject> (databaseName, setName, "IntermediateData", false, false);
                  temp.sendObject(request, errMsg);
                  temp.sendBytes(data, size, errMsg);
                  Handle<SimpleRequestResult> result = temp.getNextObject<SimpleRequestResult>(success, errMsg);
    }
    return true;

}

//tuning the backend circular buffer size
size_t PipelineStage :: getBackendCircularBufferSize (bool & success, std :: string & errMsg) {

    int backendCircularBufferSize = 1;
    if (conf->getShmSize()/conf->getPageSize()-2 < 2+2*numThreads+backendCircularBufferSize) {
        success = false;
        errMsg = "Error: Not enough buffer pool size to run the query!";
        PDB_COUT << errMsg << std :: endl;
        return 0;
    }
    backendCircularBufferSize = (conf->getShmSize()/conf->getPageSize()-4-2*numThreads);
    if (backendCircularBufferSize > 10) {
       backendCircularBufferSize = 10;
    }
    success = true;
    PDB_COUT << "backendCircularBufferSize is tuned to be " << backendCircularBufferSize << std :: endl;
    return backendCircularBufferSize;

}

//to get iterators to scan a user set
std :: vector <PageCircularBufferIteratorPtr> PipelineStage :: getUserSetIterators (HermesExecutionServer * server, bool & success, std :: string & errMsg) {

    //initialize the data proxy, scanner and set iterators
    PDBCommunicatorPtr communicatorToFrontend = make_shared<PDBCommunicator>();
    communicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);

    PDBLoggerPtr scannerLogger = make_shared<PDBLogger>("scanner.log");
    //getScanner
    int backendCircularBufferSize = getBackendCircularBufferSize (success, errMsg);
    PageScannerPtr scanner = make_shared<PageScanner>(communicatorToFrontend, shm, scannerLogger, numThreads, backendCircularBufferSize, nodeId);
     
    std :: vector<PageCircularBufferIteratorPtr> iterators;

    if (server->getFunctionality<HermesExecutionServer>().setCurPageScanner(scanner) == false) {
        success = false;
        errMsg = "Error: A job is already running!";
        std :: cout << errMsg << std :: endl;
        return iterators;
    }

    //get iterators
    PDB_COUT << "To send GetSetPages message" << std :: endl;
    iterators = scanner->getSetIterators(nodeId, jobStage->getSourceContext()->getDatabaseId(), jobStage->getSourceContext()->getTypeId(), jobStage->getSourceContext()->getSetId());
    PDB_COUT << "GetSetPages message is sent" << std :: endl;

    //return iterators
    return iterators;

}

//to create a data proxy
DataProxyPtr PipelineStage :: createProxy (int i, pthread_mutex_t connection_mutex, std :: string & errMsg) {

    //create a data proxy
    std :: string loggerName = std :: string("PipelineStage_")+std :: to_string(i);
    PDBLoggerPtr logger = make_shared<PDBLogger>(loggerName);
    pthread_mutex_lock(&connection_mutex);
    PDBCommunicatorPtr anotherCommunicatorToFrontend = make_shared<PDBCommunicator>();
    anotherCommunicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);
    pthread_mutex_unlock(&connection_mutex);
    DataProxyPtr proxy = make_shared<DataProxy>(nodeId, anotherCommunicatorToFrontend, shm, logger);
    return proxy;

}

//to execute the pipeline work defined in a TupleSetJobStage
//iterators can be empty if hash input is used
//combinerBuffers can be empty if no combining is required
void PipelineStage :: executePipelineWork (int i, SetSpecifierPtr outputSet, std :: vector<PageCircularBufferIteratorPtr> & iterators, PartitionedHashSetPtr hashSet, DataProxyPtr proxy, std :: vector <PageCircularBufferPtr> & combinerBuffers, HermesExecutionServer * server, std :: string & errMsg) {

    //setup an output page to store intermediate results and final output
    const UseTemporaryAllocationBlock tempBlock {4 * 1024 * 1024};

    PDB_COUT << i << ": to get compute plan" << std :: endl;
    Handle<ComputePlan> plan = this->jobStage->getComputePlan();
    plan->nullifyPlanPointer();
    PDB_COUT << i << ": to deep copy ComputePlan object" << std :: endl;
    Handle<ComputePlan> newPlan = deepCopyToCurrentAllocationBlock<ComputePlan>(plan);
    std :: string sourceSpecifier = jobStage->getSourceTupleSetSpecifier();
    PDB_COUT << "Source tupleset name=" << sourceSpecifier << std :: endl;
    std :: string producerComputationName = newPlan->getProducingComputationName(sourceSpecifier);
    PDB_COUT << "Producer computation name=" << producerComputationName << std :: endl;
    Handle<Computation> computation = newPlan->getPlan()->getNode(producerComputationName).getComputationHandle();


    Handle <SetIdentifier > sourceContext = this->jobStage->getSourceContext();

    //handle two types of sources
    if (sourceContext->getSetType() == UserSetType) {
        //input is a user set
        Handle<ScanUserSet<Object>> scanner = nullptr;
        if (computation->getComputationType() == "ScanUserSet") {
            scanner = unsafeCast<ScanUserSet<Object>, Computation>(computation);
        } else if (computation->getComputationType() == "SelectionComp") {
            Handle<SelectionComp<Object, Object>> selection = unsafeCast<SelectionComp<Object, Object>, Computation> (computation);
            scanner = selection->getOutputSetScanner();
        } else if (computation->getComputationType() == "MultiSelectionComp") {
            Handle<MultiSelectionComp<Object, Object>> multiSelection = 
                  unsafeCast<MultiSelectionComp<Object, Object>, Computation> (computation);
            scanner = multiSelection->getOutputSetScanner();
        } else if (computation->getComputationType() == "ClusterAggregationComp") {
            Handle<ClusterAggregateComp<Object, Object, Object, Object>> aggregator =
                  unsafeCast<ClusterAggregateComp<Object, Object, Object, Object>, Computation>(computation);
            scanner = aggregator->getOutputSetScanner();
        } else {
            std :: cout << "Error: we can't support source computation type " << computation->getComputationType() << std :: endl;
            return;
        }

        if (scanner != nullptr) {
            scanner->setIterator(iterators.at(i));
            scanner->setProxy(proxy);
            if ((scanner->getBatchSize() <= 0) || (scanner->getBatchSize() > 100)) {
                scanner->setBatchSize(batchSize);
            }
            PDB_COUT << "SCANNER BATCH SIZE: " << scanner->getBatchSize() << std :: endl;

        }
    } else {
        //input are hash tables
        Handle<ClusterAggregateComp<Object, Object, Object, Object>> aggregator = 
            unsafeCast<ClusterAggregateComp<Object, Object, Object, Object>, Computation>(computation);
        aggregator->setHashTablePointer(hashSet->getPage(i));

    }

    //handle probing
    std :: map < std :: string, ComputeInfoPtr > info;
    if ((this->jobStage->isProbing() == true) && (this->jobStage->getHashSets() != nullptr)){
        Handle<Map<String, String>> hashSetsToProbe = this->jobStage->getHashSets();
        for (PDBMapIterator<String, String> mapIter = hashSetsToProbe->begin(); mapIter != hashSetsToProbe->end(); ++mapIter) {
            std :: string key = (*mapIter).key;
            std :: string hashSetName = (*mapIter).value;
            std :: cout << "to probe " << key << ":" << hashSetName << std :: endl;
            AbstractHashSetPtr hashSet = server->getHashSet(hashSetName);
            if (hashSet == nullptr) {
                std :: cout << "ERROR in pipeline execution: broadcast data not found!" << std :: endl;
                return;
            }
            if (hashSet->getHashSetType() == "SharedHashSet") {
                SharedHashSetPtr sharedHashSet = std :: dynamic_pointer_cast<SharedHashSet> (hashSet);
                info[key] = std :: make_shared <JoinArg>(*newPlan, sharedHashSet->getPage());
            }
        }
    } else {
        //std :: cout << "info contains nothing for this stage" << std :: endl;
    }

    PDB_COUT << "source specifier: " << this->jobStage->getSourceTupleSetSpecifier() << std :: endl;
    PDB_COUT << "target specifier: " << this->jobStage->getTargetTupleSetSpecifier() << std :: endl;
    PDB_COUT << "target computation: " << this->jobStage->getTargetComputationSpecifier() << std :: endl;


    std :: string targetSpecifier = jobStage->getTargetComputationSpecifier();
    if (targetSpecifier.find("ClusterAggregationComp") != std :: string :: npos) {
                  Handle<Computation> aggComputation = newPlan->getPlan()->getNode(targetSpecifier).getComputationHandle();
                  Handle<AbstractAggregateComp> aggregate = unsafeCast<AbstractAggregateComp, Computation> (aggComputation);
                  int numPartitionsInCluster = this->jobStage->getNumTotalPartitions();
                  PDB_COUT << "num partitions in the cluster is " << numPartitionsInCluster << std :: endl;
                  aggregate->setNumNodes(jobStage->getNumNodes());
                  aggregate->setNumPartitions (numPartitionsInCluster);
                  aggregate->setBatchSize (this->batchSize);
    }

    newPlan->nullifyPlanPointer();
    std :: vector < std :: string> buildTheseTupleSets;
    jobStage->getTupleSetsToBuildPipeline (buildTheseTupleSets);
    PipelinePtr curPipeline = newPlan->buildPipeline (
                  //this->jobStage->getSourceTupleSetSpecifier(),
                  //this->jobStage->getTargetTupleSetSpecifier(),
                  //this->jobStage->getTargetComputationSpecifier(),
                  buildTheseTupleSets,
                  this->jobStage->getTargetComputationSpecifier(),
                  [] () -> std :: pair <void *, size_t> {
                      //TODO: move this to Pangea
                      PDB_COUT << "to get a new page for writing" << std :: endl;
                      void * myPage = malloc (DEFAULT_NET_PAGE_SIZE);
                      if (myPage == nullptr) {
                          std :: cout << "Pipeline Error: insufficient memory in heap" << std :: endl;
                      }
                      return std :: make_pair(myPage, DEFAULT_NET_PAGE_SIZE);
                  },

                  [] (void * page) {
                      PDB_COUT << "to discard a page" << std :: endl;
                      free (page);
                  },

                  [&] (void * page) {
                      
                      //std :: cout << "to write back a page" << std :: endl;
                      if (this->jobStage->isBroadcasting() == true) {
                          PDB_COUT << "to broadcast a page" << std :: endl;
                          //to handle a broadcast join
                          //get the objects
                          Record<Object> * record = (Record<Object> *)page;
                          //broadcast the objects
                          if (record != nullptr) {
                              Handle<Object> objectToSend = record->getRootObject();
                              Handle<JoinMap<Object>> map = unsafeCast<JoinMap<Object>, Object>(objectToSend);
                              PDB_COUT << "Map size: " << map->size() << std :: endl;
                              if (objectToSend != nullptr) {
                                  broadcastData (server, page, DEFAULT_NET_PAGE_SIZE, outputSet->getDatabase(), outputSet->getSetName(), errMsg);
                              }
                          }
                          free (page);

                      } else if ((this->jobStage->isRepartition() == true) && ( this->jobStage->isCombining() == true)) {
                          //std :: cout << "to combine a page" << std :: endl;
                          //to handle an aggregation
                          PDBPagePtr output;
                          proxy->addUserPage(outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                          memcpy(output->getBytes(), page, DEFAULT_NET_PAGE_SIZE);
                          int numNodes = jobStage->getNumNodes();
                          int k;
                          for ( k = 0; k < numNodes; k ++ ) {
                             output->incRefCount();
                          }
                          for ( k = 0; k < numNodes; k ++ ) {
                             PageCircularBufferPtr buffer = combinerBuffers[k];
                             buffer->addPageToTail(output);
                          }
                          free(page);

                      } else if ((this->jobStage->isRepartition() == true) && ( this->jobStage->isCombining() == false)) {
                          //to handle aggregation without combining
                          PDB_COUT << "to shuffle data on this page" << std :: endl;
                          //to handle an aggregation
                          Record<Vector<Handle<Vector<Handle<Object>>>>> * record = (Record<Vector<Handle<Vector<Handle<Object>>>>> *) page;
                          if (record != nullptr) {
                              Handle<Vector<Handle<Vector<Handle<Object>>>>> objectsToShuffle = record->getRootObject();
                              int numNodes = jobStage->getNumNodes();
                              int k;
                              for ( k = 0; k < numNodes; k++) {
                                  Handle<Vector<Handle<Object>>> objectToShuffle = (*objectsToShuffle)[k];
                                  if (objectToShuffle != nullptr) {
                                      //get the i-th address
                                      std :: string address = this->jobStage->getIPAddress(k);
                                      PDB_COUT << "address = " << address << std :: endl;

                                      //get the i-th port
                                      int port = this->jobStage->getPort(k);
                                      PDB_COUT << "port = " << port << std :: endl;

                                      //to shuffle data
                                      this->storeShuffleData(objectToShuffle, this->jobStage->getSinkContext()->getDatabase(), this->jobStage->getSinkContext()->getSetName(), address, port, errMsg);
                                  }
                              }
                          }
                          free(page);

                      } else {
                          std :: cout << "to write to user set" << std :: endl;
                          //to handle a vector sink
                          PDBPagePtr output = nullptr;
                          proxy->addUserPage(outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                          memcpy(output->getBytes(), page, DEFAULT_NET_PAGE_SIZE);
                          proxy->unpinUserPage(nodeId, output->getDbID(), output->getTypeID(), output->getSetID(), output);
                          free(page);
                      }
                  },

                  info
            );
   std :: cout << "\nRunning Pipeline\n";
   curPipeline->run();
   curPipeline = nullptr;
   this->jobStage->getComputePlan()->nullifyPlanPointer();

}


void PipelineStage :: runPipeline (HermesExecutionServer * server) {

    std :: vector<PageCircularBufferPtr> combinerBuffers;
    SetSpecifierPtr outputSet = make_shared<SetSpecifier>(jobStage->getSinkContext()->getDatabase(), jobStage->getSinkContext()->getSetName(), jobStage->getSinkContext()->getDatabaseId(), jobStage->getSinkContext()->getTypeId(), jobStage->getSinkContext()->getSetId());
    runPipeline (server, combinerBuffers, outputSet);

}


//combinerBuffers can be empty if the pipeline doesn't need combining
void PipelineStage :: runPipeline (HermesExecutionServer * server, std :: vector<PageCircularBufferPtr> combinerBuffers, SetSpecifierPtr outputSet) {
    //std :: cout << "Pipeline network is running" << std :: endl;
    bool success;
    std :: string errMsg;
    
    //get user set iterators
    std :: vector <PageCircularBufferIteratorPtr> iterators;
    PartitionedHashSetPtr hashSet;
    Handle <SetIdentifier > sourceContext = this->jobStage->getSourceContext();
    if (sourceContext->getSetType() == UserSetType) {
        iterators = getUserSetIterators (server, success, errMsg);
    } else {
        std :: string hashSetName = sourceContext->getDatabase() + ":" + sourceContext->getSetName();
        AbstractHashSetPtr abstractHashSet = server->getHashSet(hashSetName);
        hashSet = std :: dynamic_pointer_cast<PartitionedHashSet>(abstractHashSet);
        numThreads = hashSet->getNumPages();
    }
    //initialize mutextes 
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

                  std :: string out = getAllocator().printInactiveBlocks();
                  logger->warn(out);
                  PDB_COUT << out << std :: endl;

                  //create a data proxy
                  DataProxyPtr proxy = createProxy(i, connection_mutex, errMsg);

                  //set allocator policy
                  getAllocator().setPolicy(jobStage->getAllocatorPolicy());                  

                  //setup an output page to store intermediate results and final output
                  executePipelineWork(i, outputSet, iterators, hashSet, proxy, combinerBuffers, server, errMsg);                  

                  //restore allocator policy
                  getAllocator().setPolicy(AllocatorPolicy :: defaultAllocator);

                  callerBuzzer->buzz(PDBAlarm :: WorkAllDone, counter);

             }

         );
         worker->execute(myWork, tempBuzzer);
    }

    while (counter < numThreads) {
         tempBuzzer->wait();
    }

    counter = 0;
    pthread_mutex_destroy(&connection_mutex);

    if (server->getFunctionality<HermesExecutionServer>().setCurPageScanner(nullptr) == false) {
        success = false;
        errMsg = "Error: No job is running!";
        std :: cout << errMsg << std :: endl;
        return;
    }

    return;

}


//below method will run the combiner
void PipelineStage :: runPipelineWithShuffleSink (HermesExecutionServer * server) {
    bool success;
    std :: string errMsg;

    int numNodes = jobStage->getNumNodes();

#ifdef AUTO_TUNING
    size_t memSize = jobStage->getTotalMemoryOnThisNode();
    size_t sharedMemPoolSize = conf->getShmSize();
    size_t tunedHashPageSize = (double)(memSize*((size_t)(1024))-sharedMemPoolSize)*(0.75)/(double)(numNodes);
    if (memSize*((size_t)(1024)) < sharedMemPoolSize +  (size_t)512*(size_t)1024*(size_t)1024) {
         std :: cout << "WARNING: Auto tuning can not work for this case, we use default value" << std :: endl;
         tunedHashPageSize = conf->getHashPageSize();
    }

    std :: cout << "Tuned combiner page size is " << tunedHashPageSize << std :: endl;
    conf->setHashPageSize(tunedHashPageSize);
#endif


    size_t combinerPageSize = conf->getHashPageSize();
    //each queue has multiple producers and one consumer
    int combinerBufferSize = numThreads/numNodes;
    if (combinerBufferSize < 2) {
        combinerBufferSize = 2;
    }
    PDB_COUT << "combinerBufferSize=" << combinerBufferSize << std :: endl; 
    std :: vector <PageCircularBufferPtr> combinerBuffers;
    std :: vector <PageCircularBufferIteratorPtr> combinerIters;

    pthread_mutex_t connection_mutex;
    pthread_mutex_init(&connection_mutex, nullptr);

    //create a buzzer and counter
    PDBBuzzerPtr combinerBuzzer = make_shared<PDBBuzzer>(
         [&] (PDBAlarm myAlarm, int & combinerCounter) {
             combinerCounter ++;
             PDB_COUT << "combinerCounter = " << combinerCounter << std :: endl;
         });
    PDB_COUT << "to run combiner with " << numNodes << " threads." << std :: endl;
    int combinerCounter = 0;

    int i;
    for ( i = 0; i < numNodes; i ++ ) {
        PageCircularBufferPtr buffer = make_shared<PageCircularBuffer>(combinerBufferSize, logger);
        combinerBuffers.push_back(buffer);
        PageCircularBufferIteratorPtr iter = make_shared<PageCircularBufferIterator> (i, buffer, logger);
        combinerIters.push_back(iter);
        PDBWorkerPtr worker = server->getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
        PDB_COUT << "to run the " << i << "-th combining work..." << std :: endl;
        // start threads
        PDBWorkPtr myWork = make_shared<GenericWork> (
             [&, i] (PDBBuzzerPtr callerBuzzer) {

                  std :: string out = getAllocator().printInactiveBlocks();
                  logger->warn(out);
                  PDB_COUT << out << std :: endl;                  
                  //getAllocator().cleanInactiveBlocks((size_t)(67108844));
                  //getAllocator().cleanInactiveBlocks((size_t)(12582912));
                  getAllocator().setPolicy(noReuseAllocator);

                  //to combine data for node-i

                  std :: string errMsg;

                  //create data proxy
                  DataProxyPtr proxy = createProxy(i, connection_mutex, errMsg);

                  //get the i-th address
                  std :: string address = this->jobStage->getIPAddress(i);
                  PDB_COUT << "address = " << address << std :: endl;

                  //get the i-th port
                  int port = this->jobStage->getPort(i);
    
                  PDB_COUT << "port = " << port << std :: endl;
                  //get aggregate computation 
                  PDB_COUT << i << ": to get compute plan" << std :: endl;
                  const UseTemporaryAllocationBlock tempBlock {4 * 1024 * 1024};
                  Handle<ComputePlan> plan = this->jobStage->getComputePlan();
                  plan->nullifyPlanPointer();
                  PDB_COUT << i << ": to deep copy ComputePlan object" << std :: endl;
                  Handle<ComputePlan> newPlan = deepCopyToCurrentAllocationBlock<ComputePlan>(plan);
                  std :: string targetSpecifier = jobStage->getTargetComputationSpecifier();
                  PDB_COUT << "target computation name=" << targetSpecifier << std :: endl;
                  Handle<Computation> computation = newPlan->getPlan()->getNode(targetSpecifier).getComputationHandle();
                  Handle<AbstractAggregateComp> aggregate = unsafeCast<AbstractAggregateComp, Computation> (computation);
                  Handle<Vector<HashPartitionID>> partitions = this->jobStage->getNumPartitions(i);
                  std :: vector<HashPartitionID> stdPartitions;
                  int numPartitionsOnTheNode = partitions->size();
                  PDB_COUT << "num partitions on this node:" << numPartitionsOnTheNode << std :: endl;
                  for (int m = 0; m < numPartitionsOnTheNode; m ++) {
                      PDB_COUT << m << ":" << (*partitions)[m] << std :: endl;
                      stdPartitions.push_back((*partitions)[m]);
                  }
                  //get combiner processor
                  SimpleSingleTableQueryProcessorPtr combinerProcessor = 
                      aggregate->getCombinerProcessor(stdPartitions);
                  size_t myCombinerPageSize = combinerPageSize;
                  if (myCombinerPageSize > conf->getPageSize()-64) {
                          myCombinerPageSize = conf->getPageSize()-64;
                  }
                  void * combinerPage = (void *) malloc (myCombinerPageSize * sizeof(char));
                  std :: cout << i <<": load a combiner page with size = " << myCombinerPageSize << std :: endl;
                  combinerProcessor->loadOutputPage(combinerPage, myCombinerPageSize);

                  PageCircularBufferIteratorPtr myIter = combinerIters[i];
                  int numPages = 0;
                  while (myIter->hasNext()) {
                      PDBPagePtr page = myIter->next();
                      if (page != nullptr) {
                          //to load input page
                          numPages++;
                          combinerProcessor->loadInputPage(page->getBytes());
                          while (combinerProcessor->fillNextOutputPage()) {
                              //send out the output page
                              Record<Vector<Handle<Object>>> * record = (Record<Vector<Handle<Object>>> *)combinerPage;

                              this->storeShuffleData(record->getRootObject(), this->jobStage->getSinkContext()->getDatabase(), this->jobStage->getSinkContext()->getSetName(), address, port, errMsg); 
                              //free the output page
                              combinerProcessor->clearOutputPage();
                              free(combinerPage);
                              //allocate a new page
                              combinerPage = (void *) malloc (myCombinerPageSize * sizeof(char));
                               std :: cout << "load a combiner page with size = " << myCombinerPageSize << std :: endl;
                              //load the new page as output vector
                              combinerProcessor->loadOutputPage(combinerPage, myCombinerPageSize);

                          }
                          //unpin the input page
                          //combinerProcessor->clearInputPage();
                          page->decRefCount();
                          if (page->getRefCount() == 0) {
                               proxy->unpinUserPage (nodeId, page->getDbID(), page->getTypeID(), page->getSetID(), page);
                          }        
                      }
                  }
                  combinerProcessor->finalize();
                  combinerProcessor->fillNextOutputPage();
                  //send the output page
                  PDB_COUT << "processed " << numPages << " pages" << std :: endl;
                  Record<Vector<Handle<Object>>> * record = (Record<Vector<Handle<Object>>> *)combinerPage;
                  /*Record<Vector<Handle<Map<Object, Object>>>> * record1 = (Record<Vector<Handle<Map<Object, Object>>>> *)combinerPage;
                  Handle<Vector<Handle<Map<Object, Object>>>> mapVec = record1->getRootObject();
                  for (int l = 0; l < mapVec->size(); l++) {
                      PDB_COUT << l << "-th map size is " <<((* mapVec)[l])->size() << std :: endl;
                      PDB_COUT << l << "-th map partition id is " << ((* mapVec)[l])->getHashPartitionId() << std :: endl;
                  }
                  */
                  this->storeShuffleData(record->getRootObject(), this->jobStage->getSinkContext()->getDatabase(), this->jobStage->getSinkContext()->getSetName(), address, port, errMsg);

                  //free the output page
                  combinerProcessor->clearOutputPage();
                  free(combinerPage);
                  getAllocator().setPolicy(defaultAllocator);
                  callerBuzzer->buzz(PDBAlarm :: WorkAllDone, combinerCounter);
             }

         );
         worker->execute(myWork, combinerBuzzer);
    }
    SetSpecifierPtr outputSet = make_shared<SetSpecifier>(jobStage->getCombinerContext()->getDatabase(), jobStage->getCombinerContext()->getSetName(), jobStage->getCombinerContext()->getDatabaseId(), jobStage->getCombinerContext()->getTypeId(), jobStage->getCombinerContext()->getSetId());
    runPipeline(server, combinerBuffers, outputSet);

   
    int k;
    for ( k = 0; k < numNodes; k ++) {
         PageCircularBufferPtr buffer = combinerBuffers[k];
         buffer->close();
    }

    while (combinerCounter < numNodes) {
         combinerBuzzer->wait();
    }

    combinerCounter = 0;
    return;
}

}

#endif
