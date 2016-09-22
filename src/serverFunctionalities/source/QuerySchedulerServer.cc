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
#include "SimpleSingleTableQueryProcessor.h"
#include "InterfaceFunctions.h"
#include "QueryBase.h"
#include "PDBVector.h"
#include "Handle.h"
#include "ExecuteQuery.h"
#include "RequestResources.h"
#include "AllocatedResources.h"
#include "Selection.h"
#include "SimpleRequestHandler.h"
#include <vector>
#include <string>

namespace pdb {

QuerySchedulerServer :: ~QuerySchedulerServer () {}

QuerySchedulerServer :: QuerySchedulerServer (std :: string resourceManagerIp, int port, PDBLoggerPtr logger) {

     this->resourceManagerIp = resourceManagerIp;
     this->port = port;
     this->currentTempPlan = nullptr;
     this->currentPlan = nullptr;
     this->resources = nullptr;
     this->logger = logger;
}

std :: vector <SimpleSingleTableQueryProcessorPtr> * QuerySchedulerServer :: parseQuery(Handle<Vector<Handle<QueryBase>>> userQuery) {
     currentTempPlan = new std :: vector<SimpleSingleTableQueryProcessorPtr> ();
     for (int i = 0; i < userQuery->size(); i++) {
         Handle<Selection<Object, Object>> myQuery = unsafeCast<Selection <Object, Object>> ((*userQuery)[i]);
         currentTempPlan->push_back(myQuery->getProcessor());

     }

     return currentTempPlan;
} 

Handle<Vector<Handle<JobStage>>> QuerySchedulerServer :: parseOptimizedQuery (pdb :: Object interfaceTBD) { 
    return nullptr;
}

void QuerySchedulerServer :: registerHandlers (PDBServer &forMe) {
    //handler to request to schedule a query
    forMe.registerHandler (ExecuteQuery_TYPEID, make_shared<SimpleRequestHandler<ExecuteQuery>> (
         [&] (Handle <ExecuteQuery> request, PDBCommunicatorPtr sendUsingMe) {

         //parse the query
         const UseTemporaryAllocationBlock block1 {128 * 1024};
         std :: string errMsg;
         bool success;
         Handle <Vector <Handle<QueryBase>>> userQuery = sendUsingMe->getNextObject<Vector<Handle<QueryBase>>> (success, errMsg);
         if (!success) {
             return std :: make_pair (false, errMsg);
         }
         this->parseQuery (userQuery);

         //to query the resource manager and obtain resources
         PDBCommunicatorPtr communicatorToResourceManager = std :: make_shared<PDBCommunicator>();
         if(communicatorToResourceManager->connectToInternetServer(logger, port, resourceManagerIp, errMsg)) {
             success = false;
             std :: cout << errMsg << std :: endl;
             return std :: make_pair(success, errMsg);
         }
         
         const UseTemporaryAllocationBlock block2 {4096};
         Handle <RequestResources> resourceRequest = makeObject<RequestResources>(8, 16000);
         success = communicatorToResourceManager->sendObject<RequestResources>(resourceRequest, errMsg);
         if (!success) {
             return std :: make_pair (false, errMsg);
         }

         const UseTemporaryAllocationBlock block3 {communicatorToResourceManager->getSizeOfNextObject()};
         Handle<AllocatedResources> resourceResponse = communicatorToResourceManager->getNextObject<AllocatedResources>(success, errMsg);
    
         if (!success) {
             return std :: make_pair (false, errMsg);
         }

         this->resources = resourceResponse->getResources();

         //print out the resources
         resourceResponse->print();
 
         //to send query processor to each compute node
         //TODO 

         return std :: make_pair (true, errMsg);
      }));
    

}


}



#endif
