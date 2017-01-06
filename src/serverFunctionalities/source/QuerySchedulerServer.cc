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


#include "InterfaceFunctions.h"
#include "QuerySchedulerServer.h"
#include "QueryOutput.h"
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
#include "IrBuilder.h"
#include "DataTypes.h"
#include <vector>
#include <string>
#include <unordered_map>


namespace pdb {

QuerySchedulerServer :: ~QuerySchedulerServer () {
    pthread_mutex_destroy(&connection_mutex);
}

QuerySchedulerServer :: QuerySchedulerServer(PDBLoggerPtr logger, bool pseudoClusterMode) {
    this->logger = logger;
    this->pseudoClusterMode = pseudoClusterMode;
    pthread_mutex_init(&connection_mutex, nullptr);
}


void QuerySchedulerServer ::cleanup() {

    free(this->standardResources);
    for (int i = 0; i < currentPlan.size(); i++) {
             currentPlan[i]=nullptr;
    }
    this->currentPlan.clear();

}

QuerySchedulerServer :: QuerySchedulerServer (std :: string resourceManagerIp, int port, PDBLoggerPtr logger, bool usePipelineNetwork) {

     this->resourceManagerIp = resourceManagerIp;
     this->port = port;
     this->standardResources = nullptr;
     this->logger = logger;
     this->usePipelineNetwork = usePipelineNetwork;
}

void QuerySchedulerServer :: initialize(bool isRMRunAsServer) {

     this->standardResources = new std :: vector<StandardResourceInfoPtr>();
     if (pseudoClusterMode == false)  {
         UseTemporaryAllocationBlock(2 * 1024 * 1024);
         Handle<Vector<Handle<ResourceInfo>>> resourceObjects;
         std :: cout << "To get the resource object from the resource manager" << std :: endl;
         if (isRMRunAsServer == true) { 
             resourceObjects = getFunctionality<ResourceManagerServer>().getAllResources();
         } else {
             ResourceManagerServer rm("conf/serverlist", 8108);
             resourceObjects = rm.getAllResources();
         }

         //add and print out the resources
         for (int i = 0; i < resourceObjects->size(); i++) {

             std :: cout << i << ": address=" << (*(resourceObjects))[i]->getAddress() << ", port="<< (*(resourceObjects))[i]->getPort() <<", node="<<(*(resourceObjects))[i]->getNodeId() <<", numCores=" << (*(resourceObjects))[i]->getNumCores() << ", memSize=" << (*(resourceObjects))[i]->getMemSize() << std :: endl;
             StandardResourceInfoPtr currentResource = std :: make_shared<StandardResourceInfo>((*(resourceObjects))[i]->getNumCores(), (*(resourceObjects))[i]->getMemSize(), (*(resourceObjects))[i]->getAddress().c_str(), (*(resourceObjects))[i]->getPort(), (*(resourceObjects))[i]->getNodeId());
             this->standardResources->push_back(currentResource);
         }

     } else {
         UseTemporaryAllocationBlock(2* 1024 * 1024);
         Handle<Vector<Handle<NodeDispatcherData>>> nodeObjects;
         std :: cout << "To get the node object from the resource manager" << std :: endl;
         if (isRMRunAsServer == true) {
             nodeObjects = getFunctionality<ResourceManagerServer>().getAllNodes();
         } else {
             ResourceManagerServer rm("conf/serverlist", 8108);
             nodeObjects = rm.getAllNodes();
         }

         //add and print out the resources
         for (int i = 0; i < nodeObjects->size(); i++) {

             std :: cout << i << ": address=" << (*(nodeObjects))[i]->getAddress() << ", port="<< (*(nodeObjects))[i]->getPort() <<", node="<<(*(nodeObjects))[i]->getNodeId() << std :: endl;
             StandardResourceInfoPtr currentResource = std :: make_shared<StandardResourceInfo>(0, 0, (*(nodeObjects))[i]->getAddress().c_str(), (*(nodeObjects))[i]->getPort(), (*(nodeObjects))[i]->getNodeId());
             this->standardResources->push_back(currentResource);
         }         
    }

}

void QuerySchedulerServer :: scheduleNew() {
/*
    int counter = 0;
    PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer> (
            [&] (PDBAlarm myAlarm, int &counter) {
                   counter ++;
                std :: cout << "counter = " << counter << std :: endl;
            });
    for (int i = 0; i < this->standardResources->size(); i++) {
        PDBWorkerPtr myWorker = getWorker();
        PDBWorkPtr myWork = make_shared <GenericWork> (
            [i, this, &counter] (PDBBuzzerPtr callerBuzzer) {
                    makeObjectAllocatorBlock(1 * 1024 * 1024, true);
                    std :: cout << "to schedule on the " << i << "-th node" << std :: endl;
                    std :: cout << "port:" << (*(this->standardResources))[i]->getPort() << std :: endl;
                    std :: cout << "ip:" << (*(this->standardResources))[i]->getAddress() << std :: endl;
                    bool success = getFunctionality<QuerySchedulerServer>().scheduleNew ((*(this->standardResources))[i]->getAddress(), (*(this->standardResources))[i]->getPort(), this->logger, Recreation);
                    if (!success) {
                        callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                        return;
                    }
                    callerBuzzer->buzz (PDBAlarm :: WorkAllDone, counter);
                }
            );
        myWorker->execute(myWork, tempBuzzer);
    }

    while(counter < this->standardResources->size()) {
        tempBuzzer->wait();
    }
*/
}

bool QuerySchedulerServer :: scheduleNew(std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode) {
/*
    std :: cout << "to connect to the remote node" << std :: endl;
    PDBCommunicatorPtr communicator = std :: make_shared<PDBCommunicator>();

    std :: cout << "port:" << port << std :: endl;
    std :: cout << "ip:" << ip << std :: endl;

    string errMsg;
    bool success;
    std :: cout << "Connecting " << std :: endl;
    if(communicator->connectToInternetServer(logger, port, ip, errMsg)) {
        success = false;
        std :: cout << errMsg << std :: endl;
        return success;
    }
    std :: cout << "sending plan " << std :: endl;

    if (mode == Direct)
        success = communicator->sendObject<QueriesAndPlan>(newQueriesAndPlan, errMsg);
    else {
        Handle<QueriesAndPlan> newPlan = deepCopyToCurrentAllocationBlock<QueriesAndPlan> (newQueriesAndPlan);
        success = communicator->sendObject<QueriesAndPlan>(newPlan, errMsg);
    }
    if (!success) {
        std :: cout << errMsg << std :: endl;
        return false;
    }
*/
    return true;

}

bool QuerySchedulerServer :: schedule (std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode) {
    
    pthread_mutex_lock(&connection_mutex); 
    std :: cout << "to connect to the remote node" << std :: endl;
    PDBCommunicatorPtr communicator = std :: make_shared<PDBCommunicator>();

    std :: cout << "port:" << port << std :: endl;
    std :: cout << "ip:" << ip << std :: endl;

    string errMsg;
    bool success;
    if(communicator->connectToInternetServer(logger, port, ip, errMsg)) {
        success = false;
        std :: cout << errMsg << std :: endl;
        pthread_mutex_unlock(&connection_mutex);
        return success;
    }
    if (this->currentPlan.size() > 1) {
        std :: cout << "#####################################" << std :: endl;
        std :: cout << "WARNING: GraphIr generates 2 stages" << std :: endl;
        std :: cout << "#####################################" << std :: endl;
    }
    pthread_mutex_unlock(&connection_mutex);
    for (int i = 0; i < 1; i++) {
        Handle<JobStage> stage = currentPlan[i];
        success = schedule (stage, communicator, mode);
        if (!success) {
            return success;
        }
    }
    return true;

}

bool QuerySchedulerServer :: schedule(Handle<JobStage>& stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode) {

        bool success;
        std :: string errMsg;

        std :: cout << "to send the job stage with id=" << stage->getStageId() << " to the remote node" << std :: endl;

        if (mode == Direct) {
            stage->print();
            success = communicator->sendObject<JobStage>(stage, errMsg);
            if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return false;
            }

        } else if (mode == Recreation) {
            Handle<JobStage> stageToSend = makeObject<JobStage>(stage->getStageId());
            std :: string inDatabaseName = stage->getInput()->getDatabase();
            std :: string inSetName = stage->getInput()->getSetName();
            Handle<SetIdentifier> input = makeObject<SetIdentifier>(inDatabaseName, inSetName);
            stageToSend->setInput(input);

            std :: string outDatabaseName = stage->getOutput()->getDatabase();
            std :: string outSetName = stage->getOutput()->getSetName();
            Handle<SetIdentifier> output = makeObject<SetIdentifier>(outDatabaseName, outSetName);
            stageToSend->setOutput(output);
            stageToSend->setOutputTypeName(stage->getOutputTypeName());
 
            Vector<Handle<ExecutionOperator>> operators = stage->getOperators();
            for (int i = 0; i < operators.size(); i++) {
                Handle<QueryBase> newSelection = deepCopyToCurrentAllocationBlock<QueryBase> (operators[i]->getSelection());
                Handle<ExecutionOperator> curOperator;
                if( operators[i]->getName() == "ProjectionOperator") {
                    curOperator = makeObject<ProjectionOperator>(newSelection);
                } else if ( operators[i]->getName() == "FilterOperator") {
                    curOperator = makeObject<FilterOperator>(newSelection);
                }
                std :: cout << curOperator->getName() << std :: endl;
                stageToSend->addOperator(curOperator);
             }
             stageToSend->print();
             success = communicator->sendObject<JobStage>(stageToSend, errMsg);
             if (!success) {
                     std :: cout << errMsg << std :: endl;
                     return false;
             }
        } else if (mode == DeepCopy) {
             Handle<JobStage> stageToSend = deepCopyToCurrentAllocationBlock<JobStage> (stage);
             stageToSend->print();
             success = communicator->sendObject<JobStage>(stageToSend, errMsg);
             if (!success) {
                     std :: cout << errMsg << std :: endl;
                     return false;
             }
        } else {
             std :: cout << "Error: No such object creation mode supported in scheduler" << std :: endl;
             exit (-1);
        }
        std :: cout << "to receive query response from the remote node" << std :: endl;
        Handle<Vector<String>> result = communicator->getNextObject<Vector<String>>(success, errMsg);
        if (result != nullptr) {
            for (int j = 0; j < result->size(); j++) {
                std :: cout << "Query execute: wrote set:" << (*result)[j] << std :: endl;
            }
        }
        else {
            std :: cout << "Query execute failure: can't get results" << std :: endl;
            return false;
        }

        Vector<Handle<JobStage>> childrenStages = stage->getChildrenStages();
        for (int i = 0; i < childrenStages.size(); i ++) {
            success = schedule (childrenStages[i], communicator, mode);
            if (!success) {
                return success;
            }
        }
        return true;
}

void QuerySchedulerServer :: parseOptimizedQuery (pdb_detail::QueryGraphIrPtr queryGraph) { 

     //current logical planning only supports selection and projection
     //start from the first sink:
     //  ---we push node to this sink's pipeline until we meet a source node, a materialized node or a traversed node;
     //  ------if we meet a source node, we set input for the pipeline, and start a new pipeline stage for a new sink
     //  ------if we meet a materialized node, we set input for the pipeline, and start a new pipeline stage for the materialization node
     //  ------if we meet a traversed node, which is a materialized node, we set input for the pipeline, and start a new pipeline stage for a new sink
     //  ------if we meet a traversed node, we set the node's set as input of this pipline stage
     //if a node's parent is source, we stop here for this sink, and start from the next sink.

//     const UseTemporaryAllocationBlock tempBlock {1024*1024};
     int stageOperatorCounter = 0;
     int jobStageId = -1;
     std :: shared_ptr <pdb_detail::SetExpressionIr> curNode;
     std :: unordered_map<int, Handle<JobStage>> stageMap; 
     for (int i = 0; i < queryGraph->getSinkNodeCount(); i ++) {

            stageOperatorCounter = 0;
            curNode = queryGraph->getSinkNode(i);
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
            jobStageId ++;
            Handle<JobStage> stage = makeObject<JobStage>(jobStageId);
            stage->setOutput(output);
            bool isNodeMaterializable = true;
            while (curNode->getName() != "SourceSetNameIr") {
                if(curNode->isTraversed() == false) {
                   if(stageOperatorCounter > 0) {
                        materializationMode = curNode->getMaterializationMode();
                        if (materializationMode->isNone() == false) {
                            std :: cout << "We meet a materialization mode" << std :: endl;
                            //we meet a materialized node, we need stop this stage, set the materialized results as the input of this stage
                            //if in future, we remove the one output restriction from the pipeline, we can go on
                            Handle<SetIdentifier> input = makeObject<SetIdentifier> (materializationMode->tryGetDatabaseName( name ), materializationMode->tryGetSetName( name ));
                            stage->setInput(input);

                            //we start a new stage, which is the parent of the stopping stage
                            jobStageId ++;
                            Handle<JobStage> newStage = makeObject<JobStage>(jobStageId);
                            newStage->setOutput(input);
                            stage->setParentStage(newStage);
                            //this->currentPlan.push_back(stage);
                            stageMap[stage->getStageId()] = stage;

                            std :: cout << "stage with id=" << stage->getStageId() << " is added to map" << std :: endl;
                            std :: cout << "verify id =" << stageMap[stage->getStageId()]->getStageId() << std :: endl;

                            newStage->appendChildStage(stage);
                            stage = newStage;
                            stageOperatorCounter =0;
                            isNodeMaterializable = true;
                        }
                    }
                    // a new operator
                    if(curNode->getName() == "SelectionIr") {
                        std :: cout << "We meet a selection node" << std :: endl;
                        shared_ptr<pdb_detail::SelectionIr> selectionNode = dynamic_pointer_cast<pdb_detail::SelectionIr>(curNode);
                        Handle<ExecutionOperator> filterOp = makeObject<FilterOperator> (selectionNode->getQueryBase()); 
                        stage->addOperator(filterOp);
                        stageOperatorCounter ++;
                        curNode->setTraversed (true, jobStageId); 
                        if(curNode->isTraversed() == false) {
                            std :: cout << "Error: the node can not be modified!" << std :: endl;
                            exit(-1);
                        }  
                        curNode = selectionNode->getInputSet();
                        std :: cout << "We set the node to be traversed with id=" << jobStageId << std :: endl;
                    } else if (curNode->getName() == "ProjectionIr") {
                        std :: cout << "We meet a projection node" << std :: endl;
                        shared_ptr<pdb_detail::ProjectionIr> projectionNode = dynamic_pointer_cast<pdb_detail::ProjectionIr>(curNode);
                        if(isNodeMaterializable) {
                            Handle<QueryBase> base = projectionNode->getQueryBase();
                            Handle<Selection<Object, Object>> userQuery = unsafeCast<Selection<Object, Object>>(base);
                            stage->setOutputTypeName(userQuery->getOutputType());
                            isNodeMaterializable = false;
                        }
                        Handle<ExecutionOperator> projectionOp = makeObject<ProjectionOperator> (projectionNode->getQueryBase());
                        stage->addOperator(projectionOp);
                        stageOperatorCounter ++;
                        curNode->setTraversed (true, jobStageId); 
                        if(curNode->isTraversed() == false) {
                            std :: cout << "Error: the node can not be modified!" << std :: endl;
                            exit(-1);
                        }  
                        curNode = projectionNode->getInputSet();
                        std :: cout << "We set the node to be traversed with id=" << jobStageId << std :: endl;
                    } else {
                        std :: cout << "We only support Selection and Projection right now" << std :: endl;
                    }


                } else {
                    // TODO: we need check that this node's result must be materialized
                    // get the stage that generates the input

                    Handle<JobStage> parentStage;
                    JobStageID parentStageId = curNode->getTraversalId();
                    std :: cout << "We meet a node that has been traversed with id="<< parentStageId << std :: endl;
                    parentStage = stageMap[parentStageId];
                    parentStage->print();
                    // append this stage to that stage and finishes loop for this sink
                    Handle<SetIdentifier> input = parentStage->getOutput();
                    stage->setInput(input);
                    parentStage->appendChildStage(stage);
                    stage->setParentStage(parentStage);
                    stageMap[stage->getStageId()] = stage;
                    std :: cout << "stage with id=" << stage->getStageId() << " is added to map" << std :: endl;
                    std :: cout << "verify id =" << stageMap[stage->getStageId()]->getStageId() << std :: endl;
                    break;
                }
                std :: cout << curNode->getName() << std :: endl;

            }
            
            if (curNode->getName() == "SourceSetNameIr") {
                shared_ptr<pdb_detail::SourceSetNameIr> sourceNode = dynamic_pointer_cast<pdb_detail::SourceSetNameIr>(curNode);
                Handle<SetIdentifier> input = makeObject<SetIdentifier> (sourceNode->getDatabaseName(), sourceNode->getSetName());
                stage->setInput(input);
                stageMap[stage->getStageId()] = stage;
                std :: cout << "stage with id=" << stage->getStageId() << " is added to map" << std :: endl;
                std :: cout << "verify id =" << stageMap[stage->getStageId()]->getStageId() << std :: endl;
                this->currentPlan.push_back(stage);
            }
       }
}

void QuerySchedulerServer :: printCurrentPlan() {

         for (int i = 0; i < this->currentPlan.size(); i++) {
                 std :: cout << "#########The "<< i << "-th Plan#############"<< std :: endl;
                 currentPlan[i]->print();
         }

}

void QuerySchedulerServer :: schedule() {
         
         int counter = 0;
         PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer> (
                 [&] (PDBAlarm myAlarm, int &counter) {
                       counter ++;
                       std :: cout << "counter = " << counter << std :: endl;
                 });
         for (int i = 0; i < this->standardResources->size(); i++) {
             PDBWorkerPtr myWorker = getWorker();
             PDBWorkPtr myWork = make_shared <GenericWork> (
                 [i, this, &counter] (PDBBuzzerPtr callerBuzzer) {
                       makeObjectAllocatorBlock(1 * 1024 * 1024, true);
                       std :: cout << "to schedule on the " << i << "-th node" << std :: endl;
                       std :: cout << "port:" << (*(this->standardResources))[i]->getPort() << std :: endl;
                       std :: cout << "ip:" << (*(this->standardResources))[i]->getAddress() << std :: endl;
                       bool success = getFunctionality<QuerySchedulerServer>().schedule ((*(this->standardResources))[i]->getAddress(), (*(this->standardResources))[i]->getPort(), this->logger, Recreation);
                       if (!success) {
                              callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                              return;
                       }
                       callerBuzzer->buzz (PDBAlarm :: WorkAllDone, counter);
                 }
             );
             myWorker->execute(myWork, tempBuzzer);
         }

         while(counter < this->standardResources->size()) {
            tempBuzzer->wait();
         }

}

void QuerySchedulerServer :: registerHandlers (PDBServer &forMe) {
    //handler to request to schedule a query
    forMe.registerHandler (ExecuteQuery_TYPEID, make_shared<SimpleRequestHandler<ExecuteQuery>> (
         [&] (Handle <ExecuteQuery> request, PDBCommunicatorPtr sendUsingMe) {

         std :: string errMsg;
         bool success;

         //parse the query
         const UseTemporaryAllocationBlock block {128 * 1024 * 1024};
         std :: cout << "Got the ExecuteQuery object" << std :: endl;
         Handle <Vector <Handle<QueryBase>>> userQuery = sendUsingMe->getNextObject<Vector<Handle<QueryBase>>> (success, errMsg);
         if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return std :: make_pair (false, errMsg);
         }
         
         std :: cout << "To transform the ExecuteQuery object into a logicalGraph" << std :: endl;
         pdb_detail::QueryGraphIrPtr queryGraph = pdb_detail::buildIr(userQuery);

         std :: cout << "To transform the logicalGraph into a physical plan" << std :: endl;
         getFunctionality<QuerySchedulerServer>().parseOptimizedQuery(queryGraph); 
         getFunctionality<QuerySchedulerServer>().printCurrentPlan();
         std :: cout << "To get the resource object from the resource manager" << std :: endl;
         getFunctionality<QuerySchedulerServer>().initialize(true);
         std :: cout << "To schedule the query to run on the cluster" << std :: endl;
         getFunctionality<QuerySchedulerServer>().schedule(); 
         std :: cout << "To send back response to client" << std :: endl; 
         Handle <SimpleRequestResult> result = makeObject <SimpleRequestResult> (true, std :: string ("successfully executed query"));
         if (!sendUsingMe->sendObject (result, errMsg)) {
              return std :: make_pair (false, errMsg);
         }
         std :: cout << "to cleanup" << std :: endl;
         getFunctionality<QuerySchedulerServer>().cleanup();
         return std :: make_pair (true, errMsg);

    

      }));
    
}


}



#endif
