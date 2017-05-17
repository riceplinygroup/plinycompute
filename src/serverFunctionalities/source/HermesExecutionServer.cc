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
/**
 * Author: Jia
 * Sept 12, 2016
 */

#ifndef HERMES_EXECUTION_SERVER_CC
#define HERMES_EXECUTION_SERVER_CC

#include "PDBDebug.h"
#include "GenericWork.h"
#include "HermesExecutionServer.h"
#include "StoragePagePinned.h"
#include "StorageNoMorePage.h"
#include "SimpleRequestHandler.h"
#include "SimpleRequestResult.h"
#include "BackendTestSetScan.h"
#include "BackendTestSetCopy.h"
#include "BackendExecuteSelection.h"
#include "PageCircularBufferIterator.h"
#include "BackendSelectionWork.h"
#include "TestScanWork.h"
#include "ExecuteQuery.h"
#include "TestCopyWork.h"
#include "DataProxy.h"
#include "Selection.h"
#include "QueryBase.h"
#include "JobStage.h"
#include "TupleSetJobStage.h"
#include "AggregationJobStage.h"
#include "BroadcastJoinBuildHTJobStage.h"
#include "PipelineNetwork.h"
#include "PipelineStage.h"
#include "PartitionedHashSet.h"
#include "SharedHashSet.h"
#include "JoinMap.h"
#include <vector>


namespace pdb {


void HermesExecutionServer :: registerHandlers (PDBServer &forMe){

    forMe.registerHandler(BackendExecuteSelection_TYPEID, make_shared <SimpleRequestHandler <BackendExecuteSelection>> (
              [&] (Handle <BackendExecuteSelection> request, PDBCommunicatorPtr sendUsingMe) {
                PDB_COUT << "Start a handler to process BackendExecuteSelection messages in backend\n";
                const UseTemporaryAllocationBlock tempBlock {1024 * 128};
                {
                  bool success;
                  std :: string errMsg;
                  Handle <Vector <Handle <QueryBase>>> runUs = sendUsingMe->getNextObject <Vector <Handle <QueryBase>>> (success, errMsg);
                  if (!success) {
                    return std :: make_pair (false, errMsg);
                  }

                  // there should be only one guy
                  if (runUs->size() != 1) {
                    std :: cout << "Error: there should be exactly 1 single selection for backend to execute!" << std :: endl;
                  }
                  Handle <Selection <Object, Object>> myQuery = unsafeCast <Selection <Object, Object>> ((*runUs)[0]);
                  
                  DatabaseID dbIdIn = request->getDatabaseIn();
                  UserTypeID typeIdIn = request->getTypeIdIn();
                  SetID setIdIn = request->getSetIdIn();
                  DatabaseID dbIdOut = request->getDatabaseOut();
                  UserTypeID typeIdOut = request->getTypeIdOut();
                  SetID setIdOut = request->getSetIdOut();

                  int numThreads = getFunctionality<HermesExecutionServer>().getConf()->getNumThreads();
                  NodeID nodeId = getFunctionality<HermesExecutionServer>().getNodeID();
                  pdb :: PDBLoggerPtr logger = getFunctionality<HermesExecutionServer>().getLogger();
                  SharedMemPtr shm = getFunctionality<HermesExecutionServer>().getSharedMem();                                      
                  int backendCircularBufferSize = 3;

                  //create a scanner for input set
                  PDBCommunicatorPtr communicatorToFrontend = make_shared<PDBCommunicator>();
                  communicatorToFrontend->connectToInternetServer(logger, getFunctionality<HermesExecutionServer>().getConf()->getPort(), "localhost", errMsg);
                  PageScannerPtr scanner = make_shared<PageScanner>(communicatorToFrontend, shm, logger, numThreads, backendCircularBufferSize, nodeId);

                  if(getFunctionality<HermesExecutionServer>().setCurPageScanner(scanner) == false) {
                    success = false;
                    errMsg = "Error: A job is already running!";
                    std :: cout << errMsg << std :: endl;
                    return make_pair(success, errMsg);
                  }

                  std :: vector<PageCircularBufferIteratorPtr> iterators = scanner->getSetIterators(nodeId, dbIdIn, typeIdIn, setIdIn);

                  int numIteratorsReturned = iterators.size();
                  if( numIteratorsReturned != numThreads ) {
                    success = false;
                    errMsg = "Error: number of iterators doesn't match number of threads!";
                    std :: cout << errMsg << std :: endl;
                    return make_pair(success, errMsg);
                  }


                  int counter = 0;
                  PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>(
                    [&] (PDBAlarm myAlarm, int & counter) {
                      counter ++;
                      PDB_COUT << "counter = " << counter << std :: endl;
                    });

                  for (int i = 0; i < numThreads; i++) {
                    PDBWorkerPtr worker = getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
                    SelectionWorkPtr queryWork = make_shared<BackendSelectionWork> (iterators.at(i), dbIdOut, typeIdOut, setIdOut, &(getFunctionality<HermesExecutionServer>()), counter, myQuery);
                    worker->execute(queryWork, tempBuzzer);
                  }
                  
                  while (counter < numThreads) {
                    tempBuzzer->wait();
                  }
                  getFunctionality<HermesExecutionServer>().setCurPageScanner(nullptr);
                  // now, we notify frontend that we are done with the query
                  Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (true, std :: string("Done."));
                  if (!sendUsingMe->sendObject (response, errMsg)) {
                    return std :: make_pair (false, errMsg);
                  }
                  
                }
                return std :: make_pair(true, std :: string("Done executing query."));
              }
    ));

    //register a handler to process StoragePagePinned messages that are reponses to the same StorageGetSetPages message initiated by the current PageScanner instance.
    
    forMe.registerHandler (StoragePagePinned_TYPEID, make_shared<SimpleRequestHandler<StoragePagePinned>> (
              [&] (Handle<StoragePagePinned> request, PDBCommunicatorPtr sendUsingMe) {
                       PDB_COUT << "Start a handler to process StoragePagePinned messages\n";
                       bool res;
                       std :: string errMsg;
                       PageScannerPtr scanner = getFunctionality<HermesExecutionServer>().getCurPageScanner();
                       if(scanner == nullptr) {
                                res = false;
                                errMsg = "Fatal Error: No job is running in execution server.";
                                std :: cout << errMsg << std :: endl;
                       } else {
                                PDB_COUT << "StoragePagePinned handler: to throw pinned pages to a circular buffer!" << std :: endl;
                                scanner->recvPagesLoop(request, sendUsingMe);
                                res = true;
                       }
                       return make_pair(res, errMsg);
              }
    ));

    //register a handler to process StorageNoMorePage message, that is the final response to the StorageGetSetPages message initiated by the current PageScanner instance.

    forMe.registerHandler (StorageNoMorePage_TYPEID, make_shared<SimpleRequestHandler<StorageNoMorePage>> (
              [&] (Handle<StorageNoMorePage> request, PDBCommunicatorPtr sendUsingMe) {
                      bool res;
                      std :: string errMsg;
                      PDB_COUT << "Got StorageNoMorePage object." << std :: endl;
                      PageScannerPtr scanner = getFunctionality<HermesExecutionServer>().getCurPageScanner();
                      PDB_COUT << "To close the scanner..." << std :: endl;
                      if (scanner == nullptr) {
                              PDB_COUT << "The scanner has already been closed." << std :: endl;
                      } else {
                              scanner->closeBuffer();
                              PDB_COUT << "We closed the scanner buffer." << std :: endl;
                      }
                      res = true;                     
                      return make_pair(res, errMsg);
                

              }
    ));
      
    //register a handler to process the BackendTestSetScan message
    forMe.registerHandler (BackendTestSetScan_TYPEID, make_shared<SimpleRequestHandler<BackendTestSetScan>> (
              [&] (Handle<BackendTestSetScan> request, PDBCommunicatorPtr sendUsingMe) {
                      bool res;
                      std :: string errMsg;

                      DatabaseID dbId = request->getDatabaseID();
                      UserTypeID typeId = request->getUserTypeID();
                      SetID setId = request->getSetID();
                      PDB_COUT << "Backend received BackendTestSetScan message with dbId=" << dbId <<", typeId="<<typeId<<", setId="<<setId<<std :: endl;

                      int numThreads = getFunctionality<HermesExecutionServer>().getConf()->getNumThreads();
                      NodeID nodeId = getFunctionality<HermesExecutionServer>().getNodeID();
                      pdb :: PDBLoggerPtr logger = getFunctionality<HermesExecutionServer>().getLogger();
                      SharedMemPtr shm = getFunctionality<HermesExecutionServer>().getSharedMem();                                      
                      int backendCircularBufferSize = 3;      

                      PDBCommunicatorPtr communicatorToFrontend = make_shared<PDBCommunicator>();
                      communicatorToFrontend->connectToInternetServer(logger, getFunctionality<HermesExecutionServer>().getConf()->getPort(), "localhost", errMsg);
                      PageScannerPtr scanner = make_shared<PageScanner>(communicatorToFrontend, shm, logger, numThreads, backendCircularBufferSize, nodeId);
                       

                      if(getFunctionality<HermesExecutionServer>().setCurPageScanner(scanner) == false) {
                              res = false;
                              errMsg = "Error: A job is already running!";
                              std :: cout << errMsg << std :: endl;
                              return make_pair(res, errMsg);
                      }

                      std :: vector<PageCircularBufferIteratorPtr> iterators =
                              scanner->getSetIterators(nodeId, dbId, typeId, setId);

                      int numIteratorsReturned = iterators.size();
                      if( numIteratorsReturned != numThreads ) {
                              res = false;
                              errMsg = "Error: number of iterators doesn't match number of threads!";
                              std :: cout << errMsg << std :: endl;
                              return make_pair(res, errMsg);
                      }
                      PDB_COUT << "Buzzer is created in TestScanWork\n";
                      PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>(
                           [&] (PDBAlarm myAlarm, int & counter) {
                                    counter ++;
                                    PDB_COUT << "counter = " << counter << std :: endl;
                           });
                      int counter = 0;
                      for (int i = 0; i < numThreads; i++) {
                              PDBWorkerPtr worker = getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
                              
                              //starting processing threads;
                              TestScanWorkPtr testScanWork = make_shared<TestScanWork> (iterators.at(i), &(getFunctionality<HermesExecutionServer>()), counter);
                              worker->execute(testScanWork, tempBuzzer);
                      }

                      while (counter < numThreads) {
                               tempBuzzer->wait();
                      }

                      res = true;
                      //std :: cout << "Making response object.\n";
                      const UseTemporaryAllocationBlock block{1024};
                      Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

                      // return the result
                      res = sendUsingMe->sendObject (response, errMsg);
                      //std :: cout << "Sending response object.\n";
                      return make_pair(res, errMsg);

             }
             ));

    //register a handler to process the JobStage message
    forMe.registerHandler (JobStage_TYPEID, make_shared<SimpleRequestHandler<JobStage>> (
            [&] (Handle<JobStage> request, PDBCommunicatorPtr sendUsingMe) {
            PDB_COUT << "Backend got JobStage message with Id=" << request->getStageId() << std :: endl;
            request->print();
            bool res = true;
            std :: string errMsg;
            if( getCurPageScanner() == nullptr) { 
                //initialize a pipeline network
                //int numThreads = getFunctionality<HermesExecutionServer>().getConf()->getNumThreads();
                NodeID nodeId = getFunctionality<HermesExecutionServer>().getNodeID();
                pdb :: PDBLoggerPtr logger = getFunctionality<HermesExecutionServer>().getLogger();
                SharedMemPtr shm = getFunctionality<HermesExecutionServer>().getSharedMem();
                ConfigurationPtr conf = getFunctionality<HermesExecutionServer>().getConf();


                PipelineNetworkPtr network = make_shared<PipelineNetwork>(shm, logger, conf, nodeId, conf->getBatchSize(), conf->getNumThreads());
                PDB_COUT << "initialize the pipeline network" << std :: endl;
                network->initialize(request);
                PDB_COUT << "running source node" << std :: endl;
                network->runSource(0, this);
            } else {
                res = false;
                errMsg = "A Job is already running in this server";
            }

            PDB_COUT << "to send back reply" << std :: endl;


            //std :: cout << "Making response object.\n";
            const UseTemporaryAllocationBlock block{1024};
            Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

            // return the result
            res = sendUsingMe->sendObject (response, errMsg);
            //std :: cout << "Sending response object.\n";
            return make_pair(res, errMsg);


            }
            ));

   //register a handler to process the BroadcastJoinBuildHTJobStage message
   forMe.registerHandler (BroadcastJoinBuildHTJobStage_TYPEID, make_shared<SimpleRequestHandler<BroadcastJoinBuildHTJobStage>>(
          [&] (Handle<BroadcastJoinBuildHTJobStage> request, PDBCommunicatorPtr sendUsingMe) {
             bool success;
             std :: string errMsg;
             PDB_COUT << "Backend got Broadcast JobStage message with Id=" << request->getStageId() << std :: endl;
             request->print();

             //create a SharedHashSet instance 
             std :: cout << "pageSize = " << conf->getPageSize() << ", numPages = " << request->getNumPages() << std :: endl;
             size_t hashSetSize = conf->getPageSize()*(size_t)(request->getNumPages());
             std :: cout << "BroadcastJoinBuildHTJobStage: hashSetSize=" << hashSetSize << std :: endl;
             SharedHashSetPtr sharedHashSet = make_shared<SharedHashSet>(request->getHashSetName(), hashSetSize);
             if (sharedHashSet == nullptr) {
                 success = false;
                 errMsg = "Error: heap memory becomes insufficient";
                 std :: cout << errMsg << std :: endl;
                 // return result to frontend
                 PDB_COUT << "to send back reply" << std :: endl;
                 const UseTemporaryAllocationBlock block{1024};
                 Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (success, errMsg);
                 // return the result
                 success = sendUsingMe->sendObject (response, errMsg);
                 return make_pair(success, errMsg);

             }
             this->addHashSet(request->getHashSetName(), sharedHashSet);
             std :: cout << "BroadcastJoinBuildHTJobStage: hashSetName=" << request->getHashSetName() << std :: endl; 
             //tune backend circular buffer size
             int numThreads = 1;
             int backendCircularBufferSize = 1;
             if (conf->getShmSize()/conf->getPageSize()-2 < 2+2*numThreads+backendCircularBufferSize) {
                 success = false;
                 errMsg = "Error: Not enough buffer pool size to run the query!";
                 std :: cout << errMsg << std :: endl;
                 // return result to frontend
                 PDB_COUT << "to send back reply" << std :: endl;
                 const UseTemporaryAllocationBlock block{1024};
                 Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (success, errMsg);
                 // return the result
                 success = sendUsingMe->sendObject (response, errMsg);
                 return make_pair(success, errMsg);
             }
             backendCircularBufferSize = (conf->getShmSize()/conf->getPageSize()-4-2*numThreads);
             if (backendCircularBufferSize > 10) {
                 backendCircularBufferSize = 10;
             }
             success = true;
             PDB_COUT << "backendCircularBufferSize is tuned to be " << backendCircularBufferSize << std :: endl;

 
             //get scanner and iterators
             PDBLoggerPtr scanLogger = make_shared<PDBLogger>("agg-scanner.log");
             PDBCommunicatorPtr communicatorToFrontend = make_shared<PDBCommunicator>();
             communicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);
             PageScannerPtr scanner = make_shared<PageScanner>(communicatorToFrontend, shm, scanLogger, numThreads, backendCircularBufferSize, nodeId);
             if (getFunctionality<HermesExecutionServer>().setCurPageScanner(scanner) == false) {
                 success = false;
                 errMsg = "Error: A job is already running!";
                 std :: cout << errMsg << std :: endl;
                 // return result to frontend
                 PDB_COUT << "to send back reply" << std :: endl;
                 Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (success, errMsg);
                 // return the result
                 success = sendUsingMe->sendObject (response, errMsg);
                 return make_pair(success, errMsg);
             }

             //get iterators
             PDB_COUT << "To send GetSetPages message" << std :: endl;
             std :: vector<PageCircularBufferIteratorPtr> iterators = scanner->getSetIterators(nodeId, request->getSourceContext()->getDatabaseId(), request->getSourceContext()->getTypeId(), request->getSourceContext()->getSetId());
             PDB_COUT << "GetSetPages message is sent" << std :: endl;

             //get data proxy
             PDBCommunicatorPtr anotherCommunicatorToFrontend = make_shared<PDBCommunicator>();
             anotherCommunicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);
             DataProxyPtr proxy = make_shared<DataProxy>(nodeId, anotherCommunicatorToFrontend, shm, logger);

             //make allocator block and allocate the JoinMap
             const UseTemporaryAllocationBlock tempBlock (sharedHashSet->getPage(), hashSetSize) ;
             std :: cout << "hashSetSize = " << hashSetSize << std :: endl;
             getAllocator().setOptimizationForSpeed(true);
             //to get the sink merger
             std :: string sourceTupleSetSpecifier = request->getSourceTupleSetSpecifier();
             std :: string targetTupleSetSpecifier = request->getTargetTupleSetSpecifier();
             std :: string targetComputationSpecifier = request->getTargetComputationSpecifier();
             Handle<ComputePlan> myComputePlan = request->getComputePlan();
             SinkMergerPtr merger = myComputePlan->getMerger(sourceTupleSetSpecifier, targetTupleSetSpecifier, targetComputationSpecifier);
             Handle<Object> myMap = merger->createNewOutputContainer();

             //setup an output page to store intermediate results and final output
             PageCircularBufferIteratorPtr iter = iterators.at(0);
             PDBPagePtr page = nullptr;
             while (iter->hasNext()) {
                page = iter->next();
                if (page != nullptr) {
                    PDB_COUT << "####Scanner got a non-null page with Id = " << page->getPageID() << "for merging with Map" << std :: endl;
                    //to get the map on the page 
                    void * bytes = page->getBytes();
                    Handle<Object> theOtherMap = ((Record <Handle<Object>> *) bytes)->getRootObject();
                    Handle<JoinMap<Object>> theCastedMap = unsafeCast<JoinMap<Object>, Object>(theOtherMap);
                    //to merge the two maps
                    PDB_COUT << "####To merge the map with size = " << theCastedMap->size() << std :: endl;
                    merger->writeOut(theOtherMap, myMap);
                    PDB_COUT << "####To unpin the page with id = " << page->getPageID() << std :: endl;
                    proxy->unpinUserPage(nodeId, page->getDbID(), page->getTypeID(), page->getSetID(), page);
                    PDB_COUT << "####Unpined page with id = " << page->getPageID() << std :: endl;

               } else {
                    PDB_COUT << "####Scanner got a null page" << std :: endl;
               }
             }
             PDB_COUT << "To get record" << std :: endl;
             getRecord(myMap);

             getAllocator().setOptimizationForSpeed(false);

             if (this->setCurPageScanner(nullptr) == false) {
                success = false;
                errMsg = "Error: No job is running!";
                std :: cout << errMsg << std :: endl;
             }
             // return result to frontend
             PDB_COUT << "to send back reply" << std :: endl;
             const UseTemporaryAllocationBlock block{1024};
             Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (success, errMsg);
             // return the result
             success = sendUsingMe->sendObject (response, errMsg);
             return make_pair(success, errMsg);

          }
   ));

   //register a handler to process the AggregationJobStge message
   forMe.registerHandler (AggregationJobStage_TYPEID, make_shared<SimpleRequestHandler<AggregationJobStage>> (
          [&] (Handle<AggregationJobStage> request, PDBCommunicatorPtr sendUsingMe) {
              bool success;
              std :: string errMsg;

              PDB_COUT << "Backend got Aggregation JobStage message with Id=" << request->getStageId() << std :: endl;
              request->print();

              //get number of partitions
              int numPartitions = request->getNumNodePartitions();

              //create multiple page circular queues
              int aggregationBufferSize = 2;              
              std :: vector<PageCircularBufferPtr> hashBuffers;
              std :: vector<PageCircularBufferIteratorPtr> hashIters;

              pthread_mutex_t connection_mutex;
              pthread_mutex_init (&connection_mutex, nullptr);

              //create data proxy
              pthread_mutex_lock(&connection_mutex);
              PDBCommunicatorPtr communicatorToFrontend = make_shared<PDBCommunicator>();
              communicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);
              pthread_mutex_unlock(&connection_mutex);


              //create a buzzer and counter
              PDBBuzzerPtr hashBuzzer = make_shared<PDBBuzzer>(
                   [&] (PDBAlarm myAlarm, int & hashCounter) {
                        hashCounter ++;
                        PDB_COUT << "hashCounter = " << hashCounter << std :: endl;
              });
              std :: cout << "to run aggregation with " << numPartitions << " threads." << std :: endl;
              int hashCounter = 0;

              std :: string hashSetName = "";
              PartitionedHashSetPtr aggregationSet = nullptr;
              if (request->needsToMaterializeAggOut() == false) {
                  Handle<SetIdentifier> sinkSetIdentifier = request->getSinkContext();
                  std :: string dbName = sinkSetIdentifier->getDatabase();
                  std :: string setName = sinkSetIdentifier->getSetName();
                  hashSetName = dbName + ":" + setName;
                  aggregationSet = make_shared<PartitionedHashSet> (hashSetName, this->conf->getHashPageSize());  
                  this->addHashSet(hashSetName, aggregationSet);
              }
            

              //start multiple threads
              //each thread creates a hash set as temp set, and put key-value pairs to the hash set
              int i;
              for ( i = 0; i < numPartitions; i ++ ) {
                  PDBLoggerPtr myLogger = make_shared<PDBLogger>(std :: string("aggregation-")+std :: to_string(i));
                  PageCircularBufferPtr buffer = make_shared<PageCircularBuffer>(aggregationBufferSize, myLogger);
                  hashBuffers.push_back(buffer);
                  PageCircularBufferIteratorPtr iter = make_shared<PageCircularBufferIterator> (i, buffer, myLogger);
                  hashIters.push_back(iter);
                  PDBWorkerPtr worker = getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
                  PDB_COUT << "to run the " << i << "-th work..." << std :: endl;
                  // start threads
                  PDBWorkPtr myWork = make_shared<GenericWork> (
                     [&, i] (PDBBuzzerPtr callerBuzzer) {

                         std :: string out = getAllocator().printInactiveBlocks();
                         logger->warn(out);
                         PDB_COUT << out << std :: endl;                  
                         //getAllocator().cleanInactiveBlocks((size_t)(67108844));
                         //getAllocator().cleanInactiveBlocks((size_t)(12582912));

                         pthread_mutex_lock(&connection_mutex);
                         PDBCommunicatorPtr anotherCommunicatorToFrontend = make_shared<PDBCommunicator>();
                         anotherCommunicatorToFrontend->connectToInternetServer(logger, conf->getPort(), "localhost", errMsg);
                         pthread_mutex_unlock(&connection_mutex);
                         DataProxyPtr proxy = make_shared<DataProxy>(nodeId, anotherCommunicatorToFrontend, shm, logger);
                     
                         const UseTemporaryAllocationBlock block{4*1024*1024};
                         std :: string errMsg;

                         //get aggregate computation 
                         std :: cout << i << ": to get aggregation computation" << std :: endl;
                         Handle<AbstractAggregateComp> aggComputation = request->getAggComputation();
                         std :: cout << i << ": to deep copy aggregation computation object" << std :: endl;
                         Handle<AbstractAggregateComp> newAgg = deepCopyToCurrentAllocationBlock<AbstractAggregateComp>(aggComputation);

                         //get aggregate processor
                         SimpleSingleTableQueryProcessorPtr aggregateProcessor =
                                        newAgg->getAggregationProcessor((HashPartitionID)(i));
                         aggregateProcessor->initialize();
                         PageCircularBufferIteratorPtr myIter = hashIters[i];
                         if (request->needsToMaterializeAggOut() == false) {

                             PDBPagePtr outPage = nullptr;
                             while(myIter->hasNext()) {
                                 PDBPagePtr page = myIter->next();
                                 if (page != nullptr) {
                                     PDB_COUT << "aggregation without materialization: got one page" << std :: endl;
                                     aggregateProcessor->loadInputPage(page->getBytes());
                                     if (aggregateProcessor->needsProcessInput() == false) {
                                         continue;
                                     }
                                     if (outPage == nullptr) {
                                         //create a new partition
                                         std :: cout << "aggregation without materialization: add output page" << std :: endl; 
                                         void * bytes = aggregationSet->addPage();                                          
                                         aggregateProcessor->loadOutputPage(bytes, aggregationSet->getPageSize());
                                     }
                                     while (aggregateProcessor->fillNextOutputPage()) {
                                         aggregateProcessor->clearOutputPage();
                                         void * bytes = aggregationSet->addPage();
                                         aggregateProcessor->loadOutputPage(bytes, aggregationSet->getPageSize());
                                     }
                                                               
                                     //unpin user page
                                     aggregateProcessor->clearInputPage();
                                     page->decRefCount();
                                     if (page->getRefCount() == 0) {
                                         proxy->unpinUserPage(nodeId, page->getDbID(), page->getTypeID(), page->getSetID(), page);
                                     }
                                }

                             }
                             aggregateProcessor->finalize();
                             aggregateProcessor->fillNextOutputPage();
                             aggregateProcessor->clearOutputPage();
                            

                         } else {
                             //get output set
                             SetSpecifierPtr outputSet = make_shared<SetSpecifier>(request->getSinkContext()->getDatabase(), request->getSinkContext()->getSetName(), request->getSinkContext()->getDatabaseId(), request->getSinkContext()->getTypeId(), request->getSinkContext()->getSetId());
                             PDBPagePtr output = nullptr;

                             //aggregation page size
                             size_t aggregationPageSize = conf->getHashPageSize();

                             //allocate one output page
                             void * aggregationPage = nullptr;

                             //get aggOut processor
                             SimpleSingleTableQueryProcessorPtr aggOutProcessor =
                                        newAgg->getAggOutProcessor();
                             aggOutProcessor->initialize();
                             PageCircularBufferIteratorPtr myIter = hashIters[i];
                             while (myIter->hasNext()) {
                                 PDB_COUT << i << ": AggregationProcessor: got a page" << std :: endl;
                                 PDBPagePtr page = myIter->next();
                                 if (page != nullptr) {
                                     PDB_COUT << i << ": AggregationProcessor: got a non-null page for aggregation" << std :: endl;
                                     aggregateProcessor->loadInputPage(page->getBytes());
                                     if (aggregateProcessor->needsProcessInput() == false) {
                                         PDB_COUT << i <<": AggregationProcessor: page doesn't contain my map, we unpin it" << std :: endl;
                                         //unpin the input page 
                                         page->decRefCount();
                                         if (page->getRefCount() == 0) {
                                             proxy->unpinUserPage(nodeId, page->getDbID(), page->getTypeID(), page->getSetID(), page);
                                         }
                                         continue;
                                     }
                                     if (aggregationPage == nullptr) {
                                         PDB_COUT << i << ": AggregationProcessor: we allocated an output page" << std :: endl;
                                         aggregationPage = (void *) malloc (aggregationPageSize * sizeof(char));
                                         aggregateProcessor->loadOutputPage (aggregationPage, aggregationPageSize);
                                     }
                                     while (aggregateProcessor->fillNextOutputPage()) {
                                         PDB_COUT << i <<": AggregationProcessor: we have filled an output page" << std :: endl;
                                         //write to output set
                                         //load input page
                                         PDB_COUT << i << ": AggOutProcessor: we now have an input page" << std :: endl;
                                         aggOutProcessor->loadInputPage(aggregationPage);
                                         //get output page
                                         if (output == nullptr) {
                                             PDB_COUT << i << ": AggOutProcessor: we now pin an output page" << std :: endl;
                                             proxy->addUserPage(outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                                             aggOutProcessor->loadOutputPage (output->getBytes(), output->getSize());
                                         }
                                         while (aggOutProcessor->fillNextOutputPage()) {
                                             aggOutProcessor->clearOutputPage();
                                             PDB_COUT << i << ": AggOutProcessor: we now filled an output page and unpin it" << std :: endl;
                                             //unpin the output page
                                             proxy->unpinUserPage(nodeId, outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                                             //pin a new output page
                                             proxy->addUserPage(outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                                             //load output
                                             aggOutProcessor->loadOutputPage (output->getBytes(), output->getSize());
                                         }
                                         aggregateProcessor->clearOutputPage();
                                         aggOutProcessor->clearInputPage();
                                         free(aggregationPage);
                                         PDB_COUT << i << ": AggregationProcessor: we allocated an output page" << std :: endl;
                                         aggregationPage = (void *) malloc (aggregationPageSize * sizeof(char));
                                         aggregateProcessor->loadOutputPage (aggregationPage, aggregationPageSize);
                                     }
                                     aggregateProcessor->clearInputPage();
                                     //unpin the input page 
                                     page->decRefCount();
                                     if (page->getRefCount() == 0) {
                                         proxy->unpinUserPage(nodeId, page->getDbID(), page->getTypeID(), page->getSetID(), page);     
                                     }  
                                 }
                             }
                             if (aggregationPage != nullptr) {
                                 //finalize()
                                 aggregateProcessor->finalize();
                                 aggregateProcessor->fillNextOutputPage();
                                 //load input page
                                 PDB_COUT << i <<": AggOutProcessor: we now have the last input page" << std :: endl;
                                 aggOutProcessor->loadInputPage(aggregationPage);
                                 //get output page
                                 if (output == nullptr) {
                                      PDB_COUT << i << ": AggOutProcessor: we now pin an output page" << std :: endl;
                                      proxy->addUserPage(outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                                      aggOutProcessor->loadOutputPage (output->getBytes(), output->getSize());
                                 }
                                 while (aggOutProcessor->fillNextOutputPage()) {
                                      aggOutProcessor->clearOutputPage();
                                      PDB_COUT << i << ": AggOutProcessor: we now filled an output page and unpin it" << std :: endl;
                                      //unpin the output page
                                      proxy->unpinUserPage(nodeId, outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                                      //pin a new output page
                                      proxy->addUserPage(outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                                      //load output
                                      aggOutProcessor->loadOutputPage (output->getBytes(), output->getSize());
                                 }

                                 //finalize() and unpin last output page
                                 aggOutProcessor->finalize();
                                 aggOutProcessor->fillNextOutputPage();
                                 aggOutProcessor->clearOutputPage();
                                 PDB_COUT << i <<": AggOutProcessor: we now filled an output page and unpin it" << std :: endl;
                                 proxy->unpinUserPage(nodeId, outputSet->getDatabaseId(), outputSet->getTypeId(), outputSet->getSetId(), output);
                                 //free aggregation page
                                 aggOutProcessor->clearInputPage();
                                 aggregateProcessor->clearOutputPage();
                                 free (aggregationPage);
                            }//aggregationPage != nullptr

                         }//request->needsToMaterializeAggOut() == true
                         callerBuzzer->buzz(PDBAlarm :: WorkAllDone, hashCounter);
                         
                     }
                );
                worker->execute(myWork, hashBuzzer);

              }  //for            

             //start single-thread scanner
             //the thread iterates page, and put each page to all queues, in the end close all buffers

             int backendCircularBufferSize = numPartitions;
             int numThreads = 1;
             PDBLoggerPtr scanLogger = make_shared<PDBLogger>("agg-scanner.log");
             PageScannerPtr scanner = make_shared<PageScanner>(communicatorToFrontend, shm, scanLogger, numThreads, backendCircularBufferSize, nodeId);
             if (getFunctionality<HermesExecutionServer>().setCurPageScanner(scanner) == false) {
                 success = false;
                 errMsg = "Error: A job is already running!";
                 std :: cout << errMsg << std :: endl;
                 // return result to frontend
                 PDB_COUT << "to send back reply" << std :: endl;
                 const UseTemporaryAllocationBlock block{1024};
                 Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (success, errMsg);
                 // return the result
                 success = sendUsingMe->sendObject (response, errMsg);
                 return make_pair(success, errMsg);
             }

             //get iterators
             PDB_COUT << "To send GetSetPages message" << std :: endl;
             std :: vector<PageCircularBufferIteratorPtr> iterators = scanner->getSetIterators(nodeId, request->getSourceContext()->getDatabaseId(), request->getSourceContext()->getTypeId(), request->getSourceContext()->getSetId());
             PDB_COUT << "GetSetPages message is sent" << std :: endl;
             int numIteratorsReturned = iterators.size();
             if (numIteratorsReturned != numThreads) {
                 int k;
                 for ( k = 0; k < numPartitions; k ++) {
                     PageCircularBufferPtr buffer = hashBuffers[k];
                     buffer->close();
                 }

                 while (hashCounter < numPartitions) {
                    hashBuzzer->wait();
                 }
                 pthread_mutex_destroy(&connection_mutex);
                 success = false;
                 errMsg = "Error: number of iterators doesn't match number of threads!";
                 std :: cout << errMsg << std :: endl;
                 // return result to frontend
                 PDB_COUT << "to send back reply" << std :: endl;
                 const UseTemporaryAllocationBlock block{1024};
                 Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (success, errMsg);
                 // return the result
                 success = sendUsingMe->sendObject (response, errMsg);
                 return make_pair(success, errMsg);
             }

             //create a buzzer and counter
             PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>(
                [&] (PDBAlarm myAlarm, int & counter) {
                counter ++;
                PDB_COUT << "scan counter = " << counter << std :: endl;
             });
             int counter = 0;
 
             for (int j = 0; j < numThreads; j++) {
                PDBWorkerPtr worker = getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
                PDB_COUT << "to run the " << j << "-th scan work..." << std :: endl;
                //start threads
                PDBWorkPtr myWork = make_shared<GenericWork> (
                [&, j] (PDBBuzzerPtr callerBuzzer) {
                     //setup an output page to store intermediate results and final output
                     const UseTemporaryAllocationBlock tempBlock {4 * 1024 * 1024};
                     PageCircularBufferIteratorPtr iter = iterators.at(j);
                     PDBPagePtr page = nullptr;
                     while (iter->hasNext()) {
                         page = iter->next();
                         if (page != nullptr) {
                             PDB_COUT << "Scanner got a non-null page" << std :: endl;
                             int k;
                             for (k = 0; k < numPartitions; k++) {
                                page->incRefCount(); 
                                PDB_COUT << "add page to the " << k << "-th buffer" << std :: endl;
                                hashBuffers[k]->addPageToTail(page);
                             }
                       
                         }
                     }
                     callerBuzzer->buzz (PDBAlarm :: WorkAllDone, counter);
                } 
                );

                worker->execute(myWork, tempBuzzer);
             }
             
             while (counter < numThreads) {
                tempBuzzer->wait();
             }

             int k;
             for ( k = 0; k < numPartitions; k ++) {
                PageCircularBufferPtr buffer = hashBuffers[k];
                buffer->close();
             }
            
         
             //wait for multiple threads to return
             while (hashCounter < numPartitions) {
                 hashBuzzer->wait();
             }

             //reset scanner
             pthread_mutex_destroy(&connection_mutex);

             if (getFunctionality<HermesExecutionServer>().setCurPageScanner(nullptr) == false) {
                 success = false;
                 errMsg = "Error: No job is running!";
                 std :: cout << errMsg << std :: endl;
             }

    
             // return result to frontend
             PDB_COUT << "to send back reply" << std :: endl;
             const UseTemporaryAllocationBlock block{1024};
             Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (success, errMsg);
             // return the result
             success = sendUsingMe->sendObject (response, errMsg);
             return make_pair(success, errMsg); 


           }


    ));


    //register a handler to process the TupleSetJobStage message
    forMe.registerHandler (TupleSetJobStage_TYPEID, make_shared<SimpleRequestHandler<TupleSetJobStage>> (
            [&] (Handle<TupleSetJobStage> request, PDBCommunicatorPtr sendUsingMe) {
                PDB_COUT << "Backend got Tuple JobStage message with Id=" << request->getStageId() << std :: endl;
                request->print();
                bool res = true;
                std :: string errMsg;
                const UseTemporaryAllocationBlock block1 {4 * 1024 * 1024};
                Handle<SetIdentifier> sourceContext = request->getSourceContext();
                if( getCurPageScanner() == nullptr) {
                        NodeID nodeId = getFunctionality<HermesExecutionServer>().getNodeID();
                        pdb :: PDBLoggerPtr logger = getFunctionality<HermesExecutionServer>().getLogger();
                        SharedMemPtr shm = getFunctionality<HermesExecutionServer>().getSharedMem();
                        ConfigurationPtr conf = getFunctionality<HermesExecutionServer>().getConf();
                        Handle<PipelineStage> pipeline = makeObject<PipelineStage>(request, shm, logger, conf, nodeId, conf->getBatchSize(), conf->getNumThreads());
                        if (request->isRepartition() == false) {
                             PDB_COUT << "run pipeline..." << std :: endl;
                             pipeline->runPipeline(this);
                        } else {
                             PDB_COUT << "run pipeline with shuffling..." << std :: endl;
                             pipeline->runPipelineWithShuffleSink(this);
                        }                       
                        if ((sourceContext->isAggregationResult() == true) && (sourceContext->getSetType() == PartitionedHashSetType)) {
                             std :: string hashSetName = sourceContext->getDatabase() + ":" + sourceContext->getSetName();
                             AbstractHashSetPtr hashSet = this->getHashSet(hashSetName);
                             hashSet->cleanup(); 
                             this->removeHashSet(hashSetName);
                        }

                        //if this stage scans hash tables we need remove those hash tables
                        if (request->isProbing() == true) {
                             Handle<Map<String, String>> hashTables = request->getHashSets();
                             if (hashTables != nullptr) {
                                 for (PDBMapIterator<String, String> mapIter = hashTables->begin(); mapIter != hashTables->end(); ++mapIter) {
                                     std :: string key = (*mapIter).key;
                                     std :: string hashSetName = (*mapIter).value;
                                     std :: cout << key << ":" << hashSetName << std :: endl;
                                     AbstractHashSetPtr hashSet = this->getHashSet(hashSetName);
                                     hashSet->cleanup();
                                     this->removeHashSet(hashSetName);
                                 }
                             }
                        }
                
                } else {
                        res = false;
                        errMsg = "A Job is already running in this server";
                        std :: cout << errMsg << std :: endl;
                        //We do not remove the hash table, so that we can try again.
                }
                PDB_COUT << "to send back reply" << std :: endl;
                const UseTemporaryAllocationBlock block2 {1024};
                Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);
                // return the result
                res = sendUsingMe->sendObject (response, errMsg);
                return make_pair(res, errMsg);
            }
    ));

    //register a handler to process the BackendTestSetScan message
    forMe.registerHandler (BackendTestSetCopy_TYPEID, make_shared<SimpleRequestHandler<BackendTestSetCopy>> (
              [&] (Handle<BackendTestSetCopy> request, PDBCommunicatorPtr sendUsingMe) {
                      bool res;
                      std :: string errMsg;

                      //get input and output information
                      DatabaseID dbIdIn = request->getDatabaseIn();
                      UserTypeID typeIdIn = request->getTypeIdIn();
                      SetID setIdIn = request->getSetIdIn();
                      DatabaseID dbIdOut = request->getDatabaseOut();
                      UserTypeID typeIdOut = request->getTypeIdOut();
                      SetID setIdOut = request->getSetIdOut();
                      //std :: cout << "Backend received BackendTestSetCopy message to copy from <dbId=" << dbIdIn <<", typeId=" << typeIdIn <<", setId=" << setIdIn
                        //          << "> to < dbId=" << dbIdOut << ", typeId=" << typeIdOut << ", setId=" << setIdOut << ">" <<std :: endl;

                      int numThreads = getFunctionality<HermesExecutionServer>().getConf()->getNumThreads();
                      NodeID nodeId = getFunctionality<HermesExecutionServer>().getNodeID();
                      pdb :: PDBLoggerPtr logger = getFunctionality<HermesExecutionServer>().getLogger();
                      SharedMemPtr shm = getFunctionality<HermesExecutionServer>().getSharedMem();                                      
                      int backendCircularBufferSize = 3;


                      //create a scanner for input set
                      PDBCommunicatorPtr communicatorToFrontend = make_shared<PDBCommunicator>();
                      communicatorToFrontend->connectToInternetServer(logger, getFunctionality<HermesExecutionServer>().getConf()->getPort(), "localhost", errMsg);
                      PageScannerPtr scanner = make_shared<PageScanner>(communicatorToFrontend, shm, logger, numThreads, backendCircularBufferSize, nodeId);

   
                      if(getFunctionality<HermesExecutionServer>().setCurPageScanner(scanner) == false) {
                              res = false;
                              errMsg = "Error: A job is already running!";
                              std :: cout << errMsg << std :: endl;
                              return make_pair(res, errMsg);
                      }

                      std :: vector<PageCircularBufferIteratorPtr> iterators =
                              scanner->getSetIterators(nodeId, dbIdIn, typeIdIn, setIdIn);

                      int numIteratorsReturned = iterators.size();
                      if( numIteratorsReturned != numThreads ) {
                              res = false;
                              errMsg = "Error: number of iterators doesn't match number of threads!";
                              std :: cout << errMsg << std :: endl;
                              return make_pair(res, errMsg);
                      }


                      //create a data proxy for creating temp set
                      PDBCommunicatorPtr anotherCommunicatorToFrontend = make_shared<PDBCommunicator>();
                      anotherCommunicatorToFrontend->connectToInternetServer(logger, getFunctionality<HermesExecutionServer>().getConf()->getPort(), "localhost", errMsg);
                      DataProxyPtr proxy = make_shared<DataProxy>(nodeId, anotherCommunicatorToFrontend, shm, logger);
                      SetID tempSetId;
                      proxy->addTempSet("intermediateData", tempSetId);
                      PDB_COUT << "temp set created with setId = " << tempSetId << std :: endl;

                      //std :: cout << "Buzzer is created in TestCopyWork!\n";
                      PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>(
                           [&] (PDBAlarm myAlarm, int & counter) {
                                    counter ++;
                                    PDB_COUT << "counter = " << counter << std :: endl;
                           });
                      int counter = 0;

                      for (int i = 0; i < numThreads; i++) {
                              PDBWorkerPtr worker = getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
                              //std :: cout << "to run the " << i << "-th work..." << std :: endl;
                              //starting processing threads;
                              TestCopyWorkPtr testCopyWork = make_shared<TestCopyWork> (iterators.at(i), 0, 0, tempSetId, &(getFunctionality<HermesExecutionServer>()), counter);
                              worker->execute(testCopyWork, tempBuzzer);
                      }

                      
                      while (counter < numThreads) {
                               tempBuzzer->wait();
                      }
                      

                      counter = 0;                      
                      PDB_COUT << "All objects have been copied from set with databaseID =" << dbIdIn << ", typeID=" << typeIdIn << ", setID=" << setIdIn << std :: endl;
                      PDB_COUT << "All objects have been copied to a temp set with setID =" << tempSetId << std::endl;
                
                      //create a scanner for intermediate set
                      
                      communicatorToFrontend = make_shared<PDBCommunicator>();
                      communicatorToFrontend->connectToInternetServer(logger, getFunctionality<HermesExecutionServer>().getConf()->getPort(), "localhost", errMsg);
                      scanner = make_shared<PageScanner>(communicatorToFrontend, shm, logger, numThreads, backendCircularBufferSize, nodeId);
                      getFunctionality<HermesExecutionServer>().setCurPageScanner(nullptr);
                      getFunctionality<HermesExecutionServer>().setCurPageScanner(scanner);
                      iterators = scanner->getSetIterators(nodeId, 0, 0, tempSetId);

                      
                      PDBBuzzerPtr anotherTempBuzzer = make_shared<PDBBuzzer>(
                           [&] (PDBAlarm myAlarm, int & counter) {
                                    counter ++;
                                    PDB_COUT << "counter = " << counter << std :: endl;
                           });

                      for (int i = 0; i < numThreads; i++) {
                              PDBWorkerPtr worker = getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();

                              //starting processing threads;
                              TestCopyWorkPtr testCopyWork = make_shared<TestCopyWork> (iterators.at(i), dbIdOut, typeIdOut, setIdOut, &(getFunctionality<HermesExecutionServer>()), counter);
                              worker->execute(testCopyWork, anotherTempBuzzer);
                      }

                      while (counter < numThreads) {
                               anotherTempBuzzer->wait();
                      }

                      PDB_COUT << "All objects have been copied from a temp set with setID=" << tempSetId << std :: endl;
                      PDB_COUT << "All objects have been copied to a set with databaseID=" << dbIdOut << ", typeID=" << typeIdOut << ", setID =" << setIdOut << std::endl;
                      
                      getFunctionality<HermesExecutionServer>().setCurPageScanner(nullptr);
                      res = proxy->removeTempSet(tempSetId);
                      if (res == true) {
                           PDB_COUT << "temp set removed with setId = " << tempSetId << std :: endl;
                      } else {
                           errMsg = "Fatal error: Temp Set doesn't exist!";
                           std :: cout << errMsg << std :: endl;
                      }

                      //std :: cout << "Making response object.\n";
                      const UseTemporaryAllocationBlock block{1024};
                      Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

                      // return the result
                      res = sendUsingMe->sendObject (response, errMsg);
                      //std :: cout << "Sending response object.\n";
                      return make_pair(res, errMsg);

             }
             ));

        
} 


}


#endif
