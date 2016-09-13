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
#include "BackendTestSetScan.h"
#include "PageCircularBufferIterator.h"
#include "TestScanWork.h"
#include "MultiThreadedRequestHandler.h"
#include <vector>


namespace pdb {


void HermesExecutionServer :: registerHandlers (PDBServer &forMe){

    //register a handler to process StoragePagePinned messages that are reponses to the same StorageGetSetPages message initiated by the current PageScanner instance.
    
    forMe.registerHandler (StoragePagePinned_TYPEID, make_shared<SimpleRequestHandler<StoragePagePinned>> (
              [&] (Handle<StoragePagePinned> request, PDBCommunicatorPtr sendUsingMe) {
                       bool res;
                       std :: string errMsg;
                       PageScannerPtr scanner = getFunctionality<HermesExecutionServer>().getCurPageScanner();
                       if(scanner == nullptr) {
                                res = false;
                                errMsg = "Fatal Error: No job is running in execution server.";
                                std :: cout << errMsg << std :: endl;
                       } else {
                                scanner->recvPagesLoop(sendUsingMe);
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
                      PageScannerPtr scanner = getFunctionality<HermesExecutionServer>().getCurPageScanner();
                      if (scanner == nullptr) {
                              res = false;
                              errMsg = "Fatal Error: No job is running in execution server.";
                              std :: cout << errMsg << std :: endl;
                      } else {
                              scanner->closeBuffer();
                              res = true;
                      }                     
                      return make_pair(res, errMsg);
                

              }
    ));
  
    //register a handler to process the BackendTestSetScan message
    forMe.registerHandler (BackendTestSetScan_TYPEID, make_shared<MultiThreadedRequestHandler<BackendTestSetScan>> (
              [&] (Handle<BackendTestSetScan> request, PDBCommunicatorPtr sendUsingMe, MultiThreadedRequestHandler<BackendTestSetScan>& handler) {
                      bool res;
                      std :: string errMsg;

                      DatabaseID dbId = request->getDatabaseID();
                      UserTypeID typeId = request->getUserTypeID();
                      SetID setId = request->getSetID();

                      int numThreads = getFunctionality<HermesExecutionServer>().getConf()->getNumThreads();
                      NodeID nodeId = getFunctionality<HermesExecutionServer>().getNodeID();
                      pdb :: PDBLoggerPtr logger = getFunctionality<HermesExecutionServer>().getLogger();
                      SharedMemPtr shm = getFunctionality<HermesExecutionServer>().getSharedMem();                                      int backendCircularBufferSize = 3;      

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

                      PDBBuzzerPtr tempBuzzer{handler.getLinkedBuzzer()};

                      for (int i = 0; i < numThreads; i++) {
                              PDBWorkerPtr worker = getFunctionality<HermesExecutionServer>().getWorkers()->getWorker();
                              
                              //starting processing threads;
                              TestScanWorkPtr testScanWork = make_shared<TestScanWork> (iterators.at(i), &(getFunctionality<HermesExecutionServer>()));
                              worker->execute(testScanWork, tempBuzzer);
                      }

                      while (handler.getCounter() < numThreads) {
                               tempBuzzer->wait();
                      }
           
                      res = true;
                      return make_pair(res, errMsg);

             }
             ));

        
} 


}


#endif
