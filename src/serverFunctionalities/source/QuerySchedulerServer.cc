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
#ifndef QUERY_SCHEDULER_SERVER_CC
#define QUERY_SCHEDULER_SERVER_CC


#include "QuerySchedulerServer.h"
#include "ResourceInfo.h"
#include "ResourceManagerServer.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "InterfaceFunctions.h"
#include "QueryBase.h"
#include "PDBVector.h"
#include "Handle.h"
#include "ExecuteQuery.h"
#include "RequestResources.h"
#include "Selection.h"
#include "SimpleRequestHandler.h"
#include "SimpleRequestResult.h"
#include "GenericWork.h"
#include <vector>
#include <string>

namespace pdb {

QuerySchedulerServer :: ~QuerySchedulerServer () {}

QuerySchedulerServer :: QuerySchedulerServer (std :: string resourceManagerIp, int port, PDBLoggerPtr logger) {

     this->resourceManagerIp = resourceManagerIp;
     this->port = port;
     this->currentPlan = nullptr;
     this->resources = nullptr;
     this->logger = logger;
}


Handle<Vector<Handle<JobStage>>> QuerySchedulerServer :: parseOptimizedQuery (pdb :: Object interfaceTBD) { 
    return nullptr;
}

void QuerySchedulerServer :: registerHandlers (PDBServer &forMe) {
    //handler to request to schedule a query
    forMe.registerHandler (ExecuteQuery_TYPEID, make_shared<SimpleRequestHandler<ExecuteQuery>> (
         [&] (Handle <ExecuteQuery> request, PDBCommunicatorPtr sendUsingMe) {

         std :: string errMsg;
         bool success;

         //parse the query
             //getRecord(request);
             const UseTemporaryAllocationBlock block {128 * 1024};
             Handle<ExecuteQuery> request1 = makeObject<ExecuteQuery>();
             request1 = request;
             std :: cout << "Got the ExecuteQuery object" << std :: endl;
             Handle <Vector <Handle<QueryBase>>> userQuery = sendUsingMe->getNextObject<Vector<Handle<QueryBase>>> (success, errMsg);
             getRecord(userQuery);
             std :: cout << "Got the ExecuteQuery object" << std :: endl;
             if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return std :: make_pair (false, errMsg);
             }
         


         

         std :: cout << "To get the resource object from the resource manager" << std :: endl;
         makeObjectAllocatorBlock (1*1024*1024, true);
         this->resources = getFunctionality<ResourceManagerServer>().getAllResources();
    

         //print out the resources
         for (int i = 0; i < this->resources->size(); i++) {

             std :: cout << i << ": address=" << (*(this->resources))[i]->getAddress() << ", numCores=" << (*(this->resources))[i]->getNumCores() << ", memSize=" << (*(this->resources))[i]->getMemSize() << std :: endl;

         }
 

         //to send query processor to each compute node
         std :: vector <PDBCommunicatorPtr> * communicators = new std :: vector <PDBCommunicatorPtr>();
         for (int i = 0; i < this->resources->size(); i++) {
             
             std :: cout << "to connect to the " << i << "-th node" << std :: endl;
             PDBCommunicatorPtr communicator = std :: make_shared<PDBCommunicator>();

             std :: cout << "port:" << (*resources)[i]->getPort() << std :: endl;
             std :: cout << "ip:" << (*resources)[i]->getAddress() << std :: endl;
             if(communicator->connectToInternetServer(logger, (*resources)[i]->getPort(), (*resources)[i]->getAddress(), errMsg)) {
                  success = false;
                  std :: cout << errMsg << std :: endl;
                  return std :: make_pair(success, errMsg);
             }

             communicators->push_back(communicator);
         }
         
         std :: vector<PDBBuzzerPtr> myBuzzers(communicators->size());
         for (int i = 0; i < communicators->size(); i++) {
             PDBWorkerPtr myWorker = getWorker(); 
             PDBWorkPtr myWork = make_shared <GenericWork> (
                 [&, i] (PDBBuzzerPtr callerBuzzer) {
                  std :: cout << "to send the query object to the " << i << "-th node" << std :: endl;
                  PDBCommunicatorPtr communicator = communicators->at(i);
                  success = communicator->sendObject<ExecuteQuery>(request1, errMsg);
                  if (!success) {
                      std :: cout << errMsg << std :: endl;
                      callerBuzzer->buzz(PDBAlarm :: GenericError);
                  }
                  
 
                  success = communicator->sendObject<Vector<Handle<QueryBase>>>(userQuery, errMsg);
                  if (!success) {
                      std :: cout << errMsg << std :: endl;
                      callerBuzzer->buzz(PDBAlarm :: GenericError);
                  }
                  std :: cout << "to receive query response from the " << i << "-th node" << std :: endl;
                  const UseTemporaryAllocationBlock myBlock{communicator->getSizeOfNextObject()};
                  Handle<SimpleRequestResult> result = communicator->getNextObject<SimpleRequestResult>(success, errMsg);
                  std :: cout << "got the ack!" << std :: endl;
                  callerBuzzer->buzz (PDBAlarm :: WorkAllDone);
                 }
             ); 
             myBuzzers.push_back (make_shared <PDBBuzzer> (nullptr));
             myWorker->execute(myWork, myBuzzers[i]);
         }

         for (auto &b : myBuzzers) {
              b->wait();
         }

         return std :: make_pair (true, errMsg);

    

      }));
    

}


}



#endif
