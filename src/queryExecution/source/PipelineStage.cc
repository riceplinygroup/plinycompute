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


void PipelineStage :: runMapPipeline (HermesExecutionServer * server) {
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
                  PDBPagePtr output=nullptr;
                  PageCircularBufferIteratorPtr iter = iterators.at(i);
                  std :: cout << i << ": to get compute plan" << std :: endl;
                  Handle<ComputePlan> plan = this->jobStage->getComputePlan();
                  std :: string sourceSpecifier = jobStage->getSourceTupleSetSpecifier();
                  std :: cout << "Source tupleset name=" << sourceSpecifier << std :: endl;
                  std :: string producerComputationName = plan->getProducingComputationName(sourceSpecifier);
                  std :: cout << "Producer computation name=" << producerComputationName << std :: endl;
                  Handle<Computation> computation = plan->getPlan()->getNode(producerComputationName).getComputationHandle();
                  Handle<ScanUserSet<Object>> scanner = unsafeCast<ScanUserSet<Object>, Computation>(computation);
                  scanner->setIterator(iter);
                  scanner->setProxy(proxy);
                  scanner->setBatchSize(batchSize);
                  plan->nullifyPlanPointer();
                  PipelinePtr curPipeline = plan->buildPipeline (
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

void PipelineStage :: runMapPipelineWithShuffleSink (HermesExecutionServer * server) {

}

void PipelineStage :: runReducePipeline (HermesExecutionServer * server) {

}


void PipelineStage :: runReducePipelineWithShuffleSink (HermesExecutionServer * server) {

}


}

#endif
