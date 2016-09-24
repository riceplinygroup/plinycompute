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
         {

             const UseTemporaryAllocationBlock block {128 * 1024};
             std :: cout << "Got the ExecuteQuery object" << std :: endl;
             Handle <Vector <Handle<QueryBase>>> userQuery = sendUsingMe->getNextObject<Vector<Handle<QueryBase>>> (success, errMsg);
             std :: cout << "Got the ExecuteQuery object" << std :: endl;
             if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return std :: make_pair (false, errMsg);
             }
         }
             //this->parseQuery (userQuery);
             std :: cout << "connect to resource manager" << std :: endl;
             //to query the resource manager and obtain resources
             PDBCommunicatorPtr communicatorToResourceManager = std :: make_shared<PDBCommunicator>();
             if(communicatorToResourceManager->connectToInternetServer(logger, port, resourceManagerIp, errMsg)) {
             success = false;
             std :: cout << errMsg << std :: endl;
             return std :: make_pair(success, errMsg);
             }
         
         

         {
             std :: cout << "To send a RequestResource object to the resource manager" << std :: endl;
             const UseTemporaryAllocationBlock block {4096};
             Handle <RequestResources> resourceRequest = makeObject<RequestResources>(8, 16000);
             success = communicatorToResourceManager->sendObject<RequestResources>(resourceRequest, errMsg);
             if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return std :: make_pair (false, errMsg);
             }
         }


         {

             std :: cout << "To get the resource object from the resource manager" << std :: endl;
             const UseTemporaryAllocationBlock block {communicatorToResourceManager->getSizeOfNextObject()};
             Handle<AllocatedResources> resourceResponse = communicatorToResourceManager->getNextObject<AllocatedResources>(success, errMsg);
    
             if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return std :: make_pair (false, errMsg);
             }

             this->resources = resourceResponse->getResources();

             //print out the resources
             resourceResponse->print();
 
             //to send query processor to each compute node
             //TODO 

            return std :: make_pair (true, errMsg);

         }

      }));
    

}


}



#endif
