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
#include "PipelineNetwork.h"
#include <vector>


namespace pdb {


void HermesExecutionServer :: registerHandlers (PDBServer &forMe){

    forMe.registerHandler(BackendExecuteSelection_TYPEID, make_shared <SimpleRequestHandler <BackendExecuteSelection>> (
              [&] (Handle <BackendExecuteSelection> request, PDBCommunicatorPtr sendUsingMe) {
                std :: cout << "Start a handler to process BackendExecuteSelection messages in backend\n";
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
                      std :: cout << "counter = " << counter << std :: endl;
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
                       std :: cout << "Start a handler to process StoragePagePinned messages\n";
                       bool res;
                       std :: string errMsg;
                       PageScannerPtr scanner = getFunctionality<HermesExecutionServer>().getCurPageScanner();
                       if(scanner == nullptr) {
                                res = false;
                                errMsg = "Fatal Error: No job is running in execution server.";
                                std :: cout << errMsg << std :: endl;
                       } else {
                                //std :: cout << "to throw pinned pages to a circular buffer!" << std :: endl;
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
                      std :: cout << "Got StorageNoMorePage object." << std :: endl;
                      PageScannerPtr scanner = getFunctionality<HermesExecutionServer>().getCurPageScanner();
                      std :: cout << "To close the scanner..." << std :: endl;
                      if (scanner == nullptr) {
                              std :: cout << "The scanner has already been closed." << std :: endl;
                      } else {
                              scanner->closeBuffer();
                              std :: cout << "We closed the scanner buffer." << std :: endl;
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
                      std :: cout << "Backend received BackendTestSetScan message with dbId=" << dbId <<", typeId="<<typeId<<", setId="<<setId<<std :: endl;

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
                      std :: cout << "Buzzer is created in TestScanWork\n";
                      PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>(
                           [&] (PDBAlarm myAlarm, int & counter) {
                                    counter ++;
                                    std :: cout << "counter = " << counter << std :: endl;
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
            std :: cout << "Backend got JobStage message with Id=" << request->getStageId() << std :: endl;
            request->print();
            
            //initialize a pipeline network
            //int numThreads = getFunctionality<HermesExecutionServer>().getConf()->getNumThreads();
            NodeID nodeId = getFunctionality<HermesExecutionServer>().getNodeID();
            pdb :: PDBLoggerPtr logger = getFunctionality<HermesExecutionServer>().getLogger();
            SharedMemPtr shm = getFunctionality<HermesExecutionServer>().getSharedMem();
            ConfigurationPtr conf = getFunctionality<HermesExecutionServer>().getConf();
            PipelineNetworkPtr network = make_shared<PipelineNetwork>(shm, logger, conf, nodeId, 100, conf->getNumThreads());
            std :: cout << "initialize the pipeline network" << std :: endl;
            network->initialize(request);
            std :: cout << "running source node" << std :: endl;
            network->runSource(0, this);
            
            std :: cout << "to send back reply" << std :: endl;


            bool res = true;
            std :: string errMsg;
            //std :: cout << "Making response object.\n";
            const UseTemporaryAllocationBlock block{1024};
            Handle <SimpleRequestResult> response = makeObject <SimpleRequestResult> (res, errMsg);

            // return the result
            res = sendUsingMe->sendObject (response, errMsg);
            //std :: cout << "Sending response object.\n";
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
                      std :: cout << "temp set created with setId = " << tempSetId << std :: endl;

                      //std :: cout << "Buzzer is created in TestCopyWork!\n";
                      PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>(
                           [&] (PDBAlarm myAlarm, int & counter) {
                                    counter ++;
                                    std :: cout << "counter = " << counter << std :: endl;
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
                      std :: cout << "All objects have been copied from set with databaseID =" << dbIdIn << ", typeID=" << typeIdIn << ", setID=" << setIdIn << std :: endl;
                      std :: cout << "All objects have been copied to a temp set with setID =" << tempSetId << std::endl;
                
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
                                    std :: cout << "counter = " << counter << std :: endl;
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

                      std :: cout << "All objects have been copied from a temp set with setID=" << tempSetId << std :: endl;
                      std :: cout << "All objects have been copied to a set with databaseID=" << dbIdOut << ", typeID=" << typeIdOut << ", setID =" << setIdOut << std::endl;
                      
                      getFunctionality<HermesExecutionServer>().setCurPageScanner(nullptr);
                      res = proxy->removeTempSet(tempSetId);
                      if (res == true) {
                           std :: cout << "temp set removed with setId = " << tempSetId << std :: endl;
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
