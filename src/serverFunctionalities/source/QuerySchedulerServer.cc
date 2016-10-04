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

//by Jia, Sept 2016


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
#include "SetExpressionIr.h"
#include "SelectionIr.h"
#include "ProjectionIr.h"
#include "SourceSetNameIr.h"
#include "ProjectionOperator.h"
#include "FilterOperator.h"
#include <vector>
#include <string>
#include <unordered_map>

namespace pdb {

QuerySchedulerServer :: ~QuerySchedulerServer () {
}

void QuerySchedulerServer ::cleanup() {

    this->resources = nullptr;

}

QuerySchedulerServer :: QuerySchedulerServer (std :: string resourceManagerIp, int port, PDBLoggerPtr logger, bool usePipelineNetwork) {

     this->resourceManagerIp = resourceManagerIp;
     this->port = port;
     this->resources = nullptr;
     this->logger = logger;
     this->usePipelineNetwork = usePipelineNetwork;
}


void QuerySchedulerServer :: parseOptimizedQuery (pdb_detail::QueryGraphIr queryGraph) { 

     //current logical planning only supports selection and projection
     //start from the first sink:
     //  ---we push node to this sink's pipeline until we meet a source node, a materialized node or a traversed node;
     //  ------if we meet a source node, we set input for the pipeline, and start a new pipeline stage for a new sink
     //  ------if we meet a materialized node, we set input for the pipeline, and start a new pipeline stage for the materialization node
     //  ------if we meet a traversed node, which is a materialized node, we set input for the pipeline, and start a new pipeline stage for a new sink
     //  ------if we meet a traversed node, we set the node's set as input of this pipline stage
     //if a node's parent is source, we stop here for this sink, and start from the next sink.

     const UseTemporaryAllocationBlock tempBlock {1024*1024};
     int stageOperatorCounter = 0;
     int jobStageId = 0;
     std :: shared_ptr <pdb_detail::SetExpressionIr> curNode;
     std :: unordered_map<int, Handle<JobStage>> stageMap; 
     for (int i = 0; i < queryGraph.getSinkNodeCount(); i ++) {

            stageOperatorCounter = 0;
            curNode = queryGraph.getSinkNode(i);
            std :: cout << "the " << i << "-th sink:" << std :: endl;
            std :: cout << curNode->getName() << std :: endl;

            //the sink node must be a materialized node
            shared_ptr<pdb_detail::MaterializationMode> materializationMode = curNode->getMaterializationMode();
            if (materializationMode->isNone() == true) {
                std :: cout << "Error: sink node output must be materialized." << std :: endl;
                continue;
            } 
            string name = "";
            Handle<SetIdentifier> output = makeObject<SetIdentifier>(materializationMode->tryGetDatabaseName( name ), materializationMode->tryGetSetName( name )); 
            Handle<JobStage> stage = makeObject<JobStage>(jobStageId);
            jobStageId ++;
            stage->setOutput(output);
            while (curNode->getName() != "SourceSetNameIr") {
                if(curNode->isTraversed() == false) {
                    if (stageOperatorCounter > 0) {
                        materializationMode = curNode->getMaterializationMode();
                        if (materializationMode->isNone() == false) {

                            //we meet a materialized node, we need stop this stage, set the materialized results as the input of this stage
                            //if in future, we remove the one output restriction from the pipeline, we can go on
                            Handle<SetIdentifier> input = makeObject<SetIdentifier> (materializationMode->tryGetDatabaseName( name ), materializationMode->tryGetSetName( name ));
                            stage->setInput(input);

                            //we start a new stage, which is the parent of the stopping stage
                            Handle<JobStage> newStage = makeObject<JobStage>(jobStageId);
                            jobStageId ++;
                            newStage->setOutput(input);
                            stage->setParentStage(newStage);
                            //this->currentPlan.push_back(stage);
                            stageMap[stage->getStageId()] = stage;
                            newStage->appendChildStage(stage);
                            stage = newStage;
                        }
                    }
                    // a new operator
                    if(curNode->getName() == "SelectionIr") {
                        shared_ptr<pdb_detail::SelectionIr> selectionNode = dynamic_pointer_cast<pdb_detail::SelectionIr>(curNode);
                        Handle<FilterOperator> filterOp = makeObject<FilterOperator> (selectionNode->getQueryBase()); 
                        stage->addOperator(filterOp);
                        stageOperatorCounter ++;
                        curNode->setTraversed (true, jobStageId);   
                        curNode = selectionNode->getInputSet();
                    } else if (curNode->getName() == "ProjectionIr") {
                        shared_ptr<pdb_detail::ProjectionIr> projectionNode = dynamic_pointer_cast<pdb_detail::ProjectionIr>(curNode);
                        Handle<ProjectionOperator> projectionOp = makeObject<ProjectionOperator> (projectionNode->getQueryBase());
                        stage->addOperator(projectionOp);
                        stageOperatorCounter ++;
                        curNode->setTraversed (true, jobStageId);   
                        curNode = projectionNode->getInputSet();
                    } else {
                        std :: cout << "We only support Selection and Projection right now" << std :: endl;
                    }


                } else {
                    // TODO: we need check that this node's result must be materialized
                    // get the stage that generates the input
                    Handle<JobStage> parentStage;
                    JobStageID parentStageId = curNode->getTraversalId();
                    parentStage = stageMap[parentStageId];

                    // append this stage to that stage and finishes loop for this sink
                    Handle<SetIdentifier> input = parentStage->getOutput();
                    stage->setInput(input);
                    parentStage->appendChildStage(stage);
                    stage->setParentStage(parentStage);
                    stageMap[stage->getStageId()] = stage;
                    break;
                }
                std :: cout << curNode->getName() << std :: endl;

            }
            
            if (curNode->getName() == "SourceSetNameIr") {
                shared_ptr<pdb_detail::SourceSetNameIr> sourceNode = dynamic_pointer_cast<pdb_detail::SourceSetNameIr>(curNode);
                Handle<SetIdentifier> input = makeObject<SetIdentifier> (sourceNode->getDatabaseName(), sourceNode->getSetName());
                stage->setInput(input);
                stageMap[stage->getStageId()] = stage;
                this->currentPlan.push_back(stage);
            }
       }
}



void QuerySchedulerServer :: registerHandlers (PDBServer &forMe) {
    //handler to request to schedule a query
    forMe.registerHandler (ExecuteQuery_TYPEID, make_shared<SimpleRequestHandler<ExecuteQuery>> (
         [&] (Handle <ExecuteQuery> request, PDBCommunicatorPtr sendUsingMe) {

         std :: string errMsg;
         bool success;

         //parse the query
         const UseTemporaryAllocationBlock block {2 * 1024 * 1024};
         //Handle<ExecuteQuery> newRequest = makeObject<ExecuteQuery>();
         //std :: cout << "Got the ExecuteQuery object" << std :: endl;
         Handle <Vector <Handle<QueryBase>>> userQuery = sendUsingMe->getNextObject<Vector<Handle<QueryBase>>> (success, errMsg);
         std :: cout << "Got the ExecuteQuery object" << std :: endl;
         if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return std :: make_pair (false, errMsg);
         }
         

         std :: cout << "To get the resource object from the resource manager" << std :: endl;
         this->resources = getFunctionality<ResourceManagerServer>().getAllResources();
    

         //print out the resources
         for (int i = 0; i < this->resources->size(); i++) {

             std :: cout << i << ": address=" << (*(this->resources))[i]->getAddress() << ", numCores=" << (*(this->resources))[i]->getNumCores() << ", memSize=" << (*(this->resources))[i]->getMemSize() << std :: endl;

         }
 
/*
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
             std :: cout << "to send the query object to the " << i << "-th node" << std :: endl;
             success = communicator->sendObject<ExecuteQuery>(newRequest, errMsg);
             if (!success) {
                  std :: cout << errMsg << std :: endl;
                  return std :: make_pair(success, errMsg);
             }

             success = communicator->sendObject<Vector<Handle<QueryBase>>>(userQuery, errMsg);
             if (!success) {
                  std :: cout << errMsg << std :: endl;
                  return std :: make_pair(success, errMsg);
             }
             communicators->push_back(communicator);
         }
*/
         
         int counter = 0;
         PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer> (
                 [&] (PDBAlarm myAlarm, int &counter) {
                       counter ++;
                       std :: cout << "counter = " << counter << std :: endl;
                 });
         for (int i = 0; i < this->resources->size(); i++) {
             PDBWorkerPtr myWorker = getWorker(); 
             PDBWorkPtr myWork = make_shared <GenericWork> (
                 [&, i] (PDBBuzzerPtr callerBuzzer) {


                       std :: cout << "to connect to the " << i << "-th node" << std :: endl;
                       PDBCommunicatorPtr communicator = std :: make_shared<PDBCommunicator>();

                       std :: cout << "port:" << (*resources)[i]->getPort() << std :: endl;
                       std :: cout << "ip:" << (*resources)[i]->getAddress() << std :: endl;
                       if(communicator->connectToInternetServer(logger, (*resources)[i]->getPort(), (*resources)[i]->getAddress(), errMsg)) {
                            success = false;
                            std :: cout << errMsg << std :: endl;
                            callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                            return;
                       }

                       if(usePipelineNetwork == false) {
                           std :: cout << "to send the query object to the " << i << "-th node" << std :: endl;
                           {
                               const UseTemporaryAllocationBlock block{1024};
                               Handle<ExecuteQuery> newRequest = makeObject<ExecuteQuery>();
                               success = communicator->sendObject<ExecuteQuery>(newRequest, errMsg);
                               if (!success) {
                                   std :: cout << errMsg << std :: endl;
                                   callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                                   return;
                               }
                           }

                           Handle <Vector <Handle<QueryBase>>> newUserQuery = makeObject<Vector <Handle<QueryBase>>>();
                           *newUserQuery = *userQuery;

                           success = communicator->sendObject<Vector<Handle<QueryBase>>>(newUserQuery, errMsg);
                           if (!success) {
                               std :: cout << errMsg << std :: endl;
                               callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                               return;
                           }

                           std :: cout << "to receive query response from the " << i << "-th node" << std :: endl;
                           const UseTemporaryAllocationBlock myBlock{communicator->getSizeOfNextObject()};
                           Handle<Vector<String>> result = communicator->getNextObject<Vector<String>>(success, errMsg);
                           if (result != nullptr) {
                               for (int j = 0; j < result->size(); j++) {
                                   std :: cout << "Query execute: wrote set:" << (*result)[j] << std :: endl;
                               }
                           }
                           else {
                              std :: cout << "Query execute failure: can't get results" << std :: endl;
                              callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                              return;
                           }
                      } else {

                      }
                      callerBuzzer->buzz (PDBAlarm :: WorkAllDone, counter);
                 }
             ); 
             myWorker->execute(myWork, tempBuzzer);
         }

          
         while(counter < this->resources->size()) {
            tempBuzzer->wait();
         } 


         Handle <SimpleRequestResult> result = makeObject <SimpleRequestResult> (true, std :: string ("successfully executed query"));
         if (!sendUsingMe->sendObject (result, errMsg)) {
              return std :: make_pair (false, errMsg);
         }

         return std :: make_pair (true, errMsg);

    

      }));
    

}


}



#endif
