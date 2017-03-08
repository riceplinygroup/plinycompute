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

#include "PDBDebug.h"
#include "InterfaceFunctions.h"
#include "QuerySchedulerServer.h"
#include "DistributedStorageManagerClient.h"
#include "QueryOutput.h"
#include "ResourceInfo.h"
#include "ResourceManagerServer.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "InterfaceFunctions.h"
#include "QueryBase.h"
#include "PDBVector.h"
#include "Handle.h"
#include "ExecuteQuery.h"
#include "TupleSetExecuteQuery.h"
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
#include "ScanUserSet.h"
#include "WriteUserSet.h"
#include <vector>
#include <string>
#include <unordered_map>

namespace pdb {

QuerySchedulerServer :: ~QuerySchedulerServer () {
    pthread_mutex_destroy(&connection_mutex);
}

QuerySchedulerServer :: QuerySchedulerServer(PDBLoggerPtr logger, bool pseudoClusterMode) {
    this->port =8108;
    this->logger = logger;
    this->pseudoClusterMode = pseudoClusterMode;
    pthread_mutex_init(&connection_mutex, nullptr);
    this->jobStageId = 0;
}


QuerySchedulerServer :: QuerySchedulerServer(int port, PDBLoggerPtr logger, bool pseudoClusterMode) {
    this->port =port;
    this->logger = logger;
    this->pseudoClusterMode = pseudoClusterMode;
    pthread_mutex_init(&connection_mutex, nullptr);
    this->jobStageId = 0;
}

void QuerySchedulerServer ::cleanup() {

    free(this->standardResources);
    for (int i = 0; i < currentPlan.size(); i++) {
             currentPlan[i]=nullptr;
    }
    this->currentPlan.clear();
    for (int i = 0; i < queryPlan.size(); i++) {
             queryPlan[i]=nullptr;
    }
    this->queryPlan.clear();
}

QuerySchedulerServer :: QuerySchedulerServer (std :: string resourceManagerIp, int port, PDBLoggerPtr logger, bool usePipelineNetwork) {

     this->resourceManagerIp = resourceManagerIp;
     this->port = port;
     this->standardResources = nullptr;
     this->logger = logger;
     this->usePipelineNetwork = usePipelineNetwork;
     this->jobStageId = 0;
}

void QuerySchedulerServer :: initialize(bool isRMRunAsServer) {

     this->standardResources = new std :: vector<StandardResourceInfoPtr>();
     if (pseudoClusterMode == false)  {
         UseTemporaryAllocationBlock(2 * 1024 * 1024);
         Handle<Vector<Handle<ResourceInfo>>> resourceObjects;
         PDB_COUT << "To get the resource object from the resource manager" << std :: endl;
         if (isRMRunAsServer == true) { 
             resourceObjects = getFunctionality<ResourceManagerServer>().getAllResources();
         } else {
             ResourceManagerServer rm("conf/serverlist", 8108);
             resourceObjects = rm.getAllResources();
         }

         //add and print out the resources
         for (int i = 0; i < resourceObjects->size(); i++) {

             PDB_COUT << i << ": address=" << (*(resourceObjects))[i]->getAddress() << ", port="<< (*(resourceObjects))[i]->getPort() <<", node="<<(*(resourceObjects))[i]->getNodeId() <<", numCores=" << (*(resourceObjects))[i]->getNumCores() << ", memSize=" << (*(resourceObjects))[i]->getMemSize() << std :: endl;
             StandardResourceInfoPtr currentResource = std :: make_shared<StandardResourceInfo>((*(resourceObjects))[i]->getNumCores(), (*(resourceObjects))[i]->getMemSize(), (*(resourceObjects))[i]->getAddress().c_str(), (*(resourceObjects))[i]->getPort(), (*(resourceObjects))[i]->getNodeId());
             this->standardResources->push_back(currentResource);
         }

     } else {
         UseTemporaryAllocationBlock(2* 1024 * 1024);
         Handle<Vector<Handle<NodeDispatcherData>>> nodeObjects;
         PDB_COUT << "To get the node object from the resource manager" << std :: endl;
         if (isRMRunAsServer == true) {
             nodeObjects = getFunctionality<ResourceManagerServer>().getAllNodes();
         } else {
             ResourceManagerServer rm("conf/serverlist", 8108);
             nodeObjects = rm.getAllNodes();
         }

         //add and print out the resources
         for (int i = 0; i < nodeObjects->size(); i++) {

             PDB_COUT << i << ": address=" << (*(nodeObjects))[i]->getAddress() << ", port="<< (*(nodeObjects))[i]->getPort() <<", node="<<(*(nodeObjects))[i]->getNodeId() << std :: endl;
             StandardResourceInfoPtr currentResource = std :: make_shared<StandardResourceInfo>(0, 0, (*(nodeObjects))[i]->getAddress().c_str(), (*(nodeObjects))[i]->getPort(), (*(nodeObjects))[i]->getNodeId());
             this->standardResources->push_back(currentResource);
         }         
    }

}

/*
void QuerySchedulerServer :: scheduleNew() {
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
}
*/

/*
bool QuerySchedulerServer :: scheduleNew(std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode) {
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
    return true;

}
*/

//to replace: schedule (std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode)
bool QuerySchedulerServer :: scheduleStages (std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode) {

    pthread_mutex_lock(&connection_mutex);
    PDB_COUT << "to connect to the remote node" << std :: endl;
    PDBCommunicatorPtr communicator = std :: make_shared<PDBCommunicator>();

    PDB_COUT << "port:" << port << std :: endl;
    PDB_COUT << "ip:" << ip << std :: endl;

    string errMsg;
    bool success;
    if(communicator->connectToInternetServer(logger, port, ip, errMsg)) {
        success = false;
        std :: cout << errMsg << std :: endl;
        pthread_mutex_unlock(&connection_mutex);
        return success;
    }
    if (this->queryPlan.size() > 1) {
        PDB_COUT << "#####################################" << std :: endl;
        PDB_COUT << "WARNING: GraphIr generates 2 stages" << std :: endl;
        PDB_COUT << "#####################################" << std :: endl;
    }
    pthread_mutex_unlock(&connection_mutex);
    //Now we only allow one stage for each query graph
    for (int i = 0; i < 1; i++) {
        Handle<TupleSetJobStage> stage = queryPlan[i];
        success = scheduleStage (stage, communicator, mode);
        if (!success) {
            return success;
        }
    }
    return true;

}



//deprecated
bool QuerySchedulerServer :: schedule (std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode) {
    
    pthread_mutex_lock(&connection_mutex); 
    PDB_COUT << "to connect to the remote node" << std :: endl;
    PDBCommunicatorPtr communicator = std :: make_shared<PDBCommunicator>();

    PDB_COUT << "port:" << port << std :: endl;
    PDB_COUT << "ip:" << ip << std :: endl;

    string errMsg;
    bool success;
    if(communicator->connectToInternetServer(logger, port, ip, errMsg)) {
        success = false;
        std :: cout << errMsg << std :: endl;
        pthread_mutex_unlock(&connection_mutex);
        return success;
    }
    if (this->currentPlan.size() > 1) {
        PDB_COUT << "#####################################" << std :: endl;
        PDB_COUT << "WARNING: GraphIr generates 2 stages" << std :: endl;
        PDB_COUT << "#####################################" << std :: endl;
    }
    pthread_mutex_unlock(&connection_mutex);
    //Now we only allow one stage for each query graph
    for (int i = 0; i < 1; i++) {
        Handle<JobStage> stage = currentPlan[i];
        success = schedule (stage, communicator, mode);
        if (!success) {
            return success;
        }
    }
    return true;

}

//to replace: schedule(Handle<JobStage>& stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode)
bool QuerySchedulerServer :: scheduleStage(Handle<TupleSetJobStage>& stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode) {
        bool success;
        std :: string errMsg;
        Handle<ComputePlan> plan = stage->getComputePlan();
        plan->nullifyPlanPointer();
        PDB_COUT << "to send the job stage with id=" << stage->getStageId() << " to the remote node" << std :: endl;

        if (mode == Direct) {
            stage->print();
            success = communicator->sendObject<TupleSetJobStage>(stage, errMsg);
            if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return false;
            }
        }else if (mode == DeepCopy) {
             Handle<TupleSetJobStage> stageToSend = deepCopyToCurrentAllocationBlock<TupleSetJobStage> (stage);
             stageToSend->print();
             success = communicator->sendObject<TupleSetJobStage>(stageToSend, errMsg);
             if (!success) {
                     std :: cout << errMsg << std :: endl;
                     return false;
             }
        } else {
             std :: cout << "Error: No such object creation mode supported in query scheduler" << std :: endl;
             return false;
        }
        PDB_COUT << "to receive query response from the remote node" << std :: endl;
        Handle<Vector<String>> result = communicator->getNextObject<Vector<String>>(success, errMsg);
        if (result != nullptr) {
            for (int j = 0; j < result->size(); j++) {
                PDB_COUT << "Query execute: wrote set:" << (*result)[j] << std :: endl;
            }
        }
        else {
            PDB_COUT << "Query execute failure: can't get results" << std :: endl;
            return false;
        }

        return true;

}


//deprecated
bool QuerySchedulerServer :: schedule(Handle<JobStage>& stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode) {

        bool success;
        std :: string errMsg;

        PDB_COUT << "to send the job stage with id=" << stage->getStageId() << " to the remote node" << std :: endl;

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
                PDB_COUT << curOperator->getName() << std :: endl;
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
             return false;
        }
        PDB_COUT << "to receive query response from the remote node" << std :: endl;
        Handle<Vector<String>> result = communicator->getNextObject<Vector<String>>(success, errMsg);
        if (result != nullptr) {
            for (int j = 0; j < result->size(); j++) {
                PDB_COUT << "Query execute: wrote set:" << (*result)[j] << std :: endl;
            }
        }
        else {
            PDB_COUT << "Query execute failure: can't get results" << std :: endl;
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

String QuerySchedulerServer :: transformQueryToTCAP (Vector<Handle<Computation>> myComputations) {

        //TODO: convert below placeholder to query analysis logic
/*        String myTCAPString =
               "inputData (in) <= SCAN ('mySet', 'myData', 'ScanSet_0') \n\
                inputWithAtt (in, att) <= APPLY (inputData (in), inputData (in), 'SelectionComp_1', 'methodCall_1') \n\
                inputWithAttAndMethod (in, att, method) <= APPLY (inputWithAtt (in), inputWithAtt (in, att), 'SelectionComp_1', 'attAccess_2') \n\
                inputWithBool (in, bool) <= APPLY (inputWithAttAndMethod (att, method), inputWithAttAndMethod (in), 'SelectionComp_1', '==_0') \n\
                filteredInput (in) <= FILTER (inputWithBool (bool), inputWithBool (in), 'SelectionComp_1') \n\
                projectedInputWithPtr (out) <= APPLY (filteredInput (in), filteredInput (), 'SelectionComp_1', 'methodCall_4') \n\
                projectedInput (out) <= APPLY (projectedInputWithPtr (out), projectedInputWithPtr (), 'SelectionComp_1', 'deref_3') \n\
                aggWithKeyWithPtr (out, key) <= APPLY (projectedInput (out), projectedInput (out), 'AggregationComp_2', 'attAccess_1') \n\
                aggWithKey (out, key) <= APPLY (aggWithKeyWithPtr (key), aggWithKeyWithPtr (out), 'AggregationComp_2', 'deref_0') \n\
                aggWithValue (key, value) <= APPLY (aggWithKey (out), aggWithKey (key), 'AggregationComp_2', 'methodCall_2') \n\
                agg (aggOut) <= AGGREGATE (aggWithValue (key, value), 'AggregationComp_2') \n\
                checkSales (aggOut, isSales) <= APPLY (agg (aggOut), agg (aggOut), 'SelectionComp_3', 'methodCall_0') \n\
                justSales (aggOut, isSales) <= FILTER (checkSales (isSales), checkSales (aggOut), 'SelectionComp_3') \n\
                final (result) <= APPLY (justSales (aggOut), justSales (), 'SelectionComp_3', 'methodCall_1') \n\
                nothing () <= OUTPUT (final (result), 'outSet', 'myDB', 'SetWriter_4')";
*/

        String myTCAPString = 
                "inputData (in) <= SCAN ('chris_set', 'chris_db', 'ScanUserSet_0') \n\
                checkFrank (in, isFrank) <= APPLY (inputData (in), inputData (in), 'SelectionComp_1', 'methodCall_0') \n\
                justFrank (in, isFrank) <= FILTER (checkFrank(isFrank), checkFrank(in), 'SelectionComp_1') \n\
                projectedInputWithPtr (out) <= APPLY (justFrank (in), justFrank (), 'SelectionComp_1', 'methodCall_2') \n\
                projectedInput (out) <= APPLY (projectedInputWithPtr (out), projectedInputWithPtr (), 'SelectionComp_1', 'deref_1') \n\
                nothing() <= OUTPUT (projectedInput (out), 'output_set1', 'chris_db', 'WriteUserSet_2')";
        return myTCAPString;
}



//to replace parseOptimizedQuery
void QuerySchedulerServer :: parseQuery(Vector<Handle<Computation>> myComputations, String myTCAPString) {
    //TODO: to replace the below placeholder using real logic
    //TODO: to analyze the logical plan and output a vector of TupleSetJobStage instances
    Handle<ComputePlan> myPlan = makeObject<ComputePlan> (myTCAPString, myComputations);
    Handle<TupleSetJobStage> jobStage = makeObject<TupleSetJobStage>(jobStageId);
    jobStageId ++;
    jobStage->setComputePlan(myPlan, "inputData", "projectedInput", "WriteUserSet_2");
    std :: string sourceSpecifier = "ScanUserSet_0";
    Handle<Computation> sourceComputation = myPlan->getPlan()->getNode(sourceSpecifier).getComputationHandle();
    Handle<ScanUserSet<Object>> scanner = unsafeCast<ScanUserSet<Object>, Computation>(sourceComputation);
    Handle<SetIdentifier> source = makeObject<SetIdentifier>(scanner->getDatabaseName(), scanner->getSetName());
    std :: string sinkSpecifier = "WriteUserSet_2";
    Handle<Computation> sinkComputation = myPlan->getPlan()->getNode(sinkSpecifier).getComputationHandle();
    Handle<WriteUserSet<Object>> writer = unsafeCast<WriteUserSet<Object>, Computation>(sinkComputation);
    Handle<SetIdentifier> sink = makeObject<SetIdentifier>(writer->getDatabaseName(), writer->getSetName());
    jobStage->setSourceContext(source);
    jobStage->setSinkContext(sink);
    jobStage->setOutputTypeName(writer->getOutputType());
    this->queryPlan.push_back(jobStage);
}


//deprecated
//checkSet can only be true if we deploy QuerySchedulerServer, CatalogServer and DistributedStorageManagerServer on the same machine.
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
            PDB_COUT << "the " << i << "-th sink:" << std :: endl;
            PDB_COUT << curNode->getName() << std :: endl;

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
                            PDB_COUT << "We meet a materialization mode" << std :: endl;
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

                            PDB_COUT << "stage with id=" << stage->getStageId() << " is added to map" << std :: endl;
                            PDB_COUT << "verify id =" << stageMap[stage->getStageId()]->getStageId() << std :: endl;

                            newStage->appendChildStage(stage);
                            stage = newStage;
                            stageOperatorCounter =0;
                            isNodeMaterializable = true;
                        }
                    }
                    // a new operator
                    if(curNode->getName() == "SelectionIr") {
                        PDB_COUT << "We meet a selection node" << std :: endl;
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
                        PDB_COUT << "We set the node to be traversed with id=" << jobStageId << std :: endl;
                    } else if (curNode->getName() == "ProjectionIr") {
                        PDB_COUT << "We meet a projection node" << std :: endl;
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
                        PDB_COUT << "We set the node to be traversed with id=" << jobStageId << std :: endl;
                    } else {
                        PDB_COUT << "We only support Selection and Projection right now" << std :: endl;
                    }


                } else {
                    // TODO: we need check that this node's result must be materialized
                    // get the stage that generates the input

                    Handle<JobStage> parentStage;
                    JobStageID parentStageId = curNode->getTraversalId();
                    PDB_COUT << "We meet a node that has been traversed with id="<< parentStageId << std :: endl;
                    parentStage = stageMap[parentStageId];
                    parentStage->print();
                    // append this stage to that stage and finishes loop for this sink
                    Handle<SetIdentifier> input = parentStage->getOutput();
                    stage->setInput(input);
                    parentStage->appendChildStage(stage);
                    stage->setParentStage(parentStage);
                    stageMap[stage->getStageId()] = stage;
                    PDB_COUT << "stage with id=" << stage->getStageId() << " is added to map" << std :: endl;
                    PDB_COUT << "verify id =" << stageMap[stage->getStageId()]->getStageId() << std :: endl;
                    break;
                }
                PDB_COUT << curNode->getName() << std :: endl;

            }
            
            if (curNode->getName() == "SourceSetNameIr") {
                shared_ptr<pdb_detail::SourceSetNameIr> sourceNode = dynamic_pointer_cast<pdb_detail::SourceSetNameIr>(curNode);
                Handle<SetIdentifier> input = makeObject<SetIdentifier> (sourceNode->getDatabaseName(), sourceNode->getSetName());
                stage->setInput(input);
                stageMap[stage->getStageId()] = stage;
                PDB_COUT << "stage with id=" << stage->getStageId() << " is added to map" << std :: endl;
                PDB_COUT << "verify id =" << stageMap[stage->getStageId()]->getStageId() << std :: endl;
                this->currentPlan.push_back(stage);
            }
       }
}

//to replace: printCurrentPlan()
void QuerySchedulerServer :: printStages() {

         for(int i = 0; i < this->queryPlan.size(); i++) {
                PDB_COUT << "#########The "<< i << "-th Plan#############"<< std :: endl;
                queryPlan[i]->print();
         }

}


//deprecated
void QuerySchedulerServer :: printCurrentPlan() {

         for (int i = 0; i < this->currentPlan.size(); i++) {
                 PDB_COUT << "#########The "<< i << "-th Plan#############"<< std :: endl;
                 currentPlan[i]->print();
         }

}


//to replace: schedule()
void QuerySchedulerServer :: scheduleQuery() {
 
         int counter = 0;
         PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer> (
                 [&] (PDBAlarm myAlarm, int &counter) {
                       counter ++;
                       PDB_COUT << "counter = " << counter << std :: endl;
                 });
         for (int i = 0; i < this->standardResources->size(); i++) {
             PDBWorkerPtr myWorker = getWorker();
             PDBWorkPtr myWork = make_shared <GenericWork> (
                 [i, this, &counter] (PDBBuzzerPtr callerBuzzer) {
                       makeObjectAllocatorBlock(1 * 1024 * 1024, true);
                       PDB_COUT << "to schedule on the " << i << "-th node" << std :: endl;
                       PDB_COUT << "port:" << (*(this->standardResources))[i]->getPort() << std :: endl;
                       PDB_COUT << "ip:" << (*(this->standardResources))[i]->getAddress() << std :: endl;
                       bool success = getFunctionality<QuerySchedulerServer>().scheduleStages ((*(this->standardResources))[i]->getAddress(), (*(this->standardResources))[i]->getPort(), this->logger, DeepCopy);
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


//deprecated
void QuerySchedulerServer :: schedule() {
         
         int counter = 0;
         PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer> (
                 [&] (PDBAlarm myAlarm, int &counter) {
                       counter ++;
                       PDB_COUT << "counter = " << counter << std :: endl;
                 });
         for (int i = 0; i < this->standardResources->size(); i++) {
             PDBWorkerPtr myWorker = getWorker();
             PDBWorkPtr myWork = make_shared <GenericWork> (
                 [i, this, &counter] (PDBBuzzerPtr callerBuzzer) {
                       makeObjectAllocatorBlock(1 * 1024 * 1024, true);
                       PDB_COUT << "to schedule on the " << i << "-th node" << std :: endl;
                       PDB_COUT << "port:" << (*(this->standardResources))[i]->getPort() << std :: endl;
                       PDB_COUT << "ip:" << (*(this->standardResources))[i]->getAddress() << std :: endl;
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

    //handler to schedule a TupleSet-based query 
    forMe.registerHandler (TupleSetExecuteQuery_TYPEID, make_shared<SimpleRequestHandler<TupleSetExecuteQuery>> (
         [&] (Handle <TupleSetExecuteQuery> request, PDBCommunicatorPtr sendUsingMe) {
         
             std :: string errMsg;
             bool success;

             //parse the query
             const UseTemporaryAllocationBlock block {128 * 1024 * 1024};
             PDB_COUT << "Got the TupleSetExecuteQuery object" << std :: endl;
             Handle <Vector <Handle<Computation>>> userQuery = sendUsingMe->getNextObject<Vector<Handle<Computation>>> (success, errMsg);
             if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return std :: make_pair (false, errMsg);
             }
             PDB_COUT << "To transform the ExecuteQuery object into a TCAP string" << std :: endl;
             String tcapString = getFunctionality<QuerySchedulerServer>().transformQueryToTCAP(*userQuery);

             PDB_COUT << "To transform the TCAP string into a physical plan" << std :: endl;
             getFunctionality<QuerySchedulerServer>().parseQuery(*userQuery, tcapString);

             getFunctionality<QuerySchedulerServer>().printStages();
             PDB_COUT << "To get the resource object from the resource manager" << std :: endl;
             getFunctionality<QuerySchedulerServer>().initialize(true);
             PDB_COUT << "To schedule the query to run on the cluster" << std :: endl;
             getFunctionality<QuerySchedulerServer>().scheduleQuery();
             PDB_COUT << "To send back response to client" << std :: endl;
             Handle <SimpleRequestResult> result = makeObject <SimpleRequestResult> (true, std :: string ("successfully executed query"));
             if (!sendUsingMe->sendObject (result, errMsg)) {
                 return std :: make_pair (false, errMsg);
             }
             PDB_COUT << "to cleanup" << std :: endl;
             getFunctionality<QuerySchedulerServer>().cleanup();
             return std :: make_pair (true, errMsg);

         }
    ));

    //deprecated
    //handler to schedule a query
    forMe.registerHandler (ExecuteQuery_TYPEID, make_shared<SimpleRequestHandler<ExecuteQuery>> (
         [&] (Handle <ExecuteQuery> request, PDBCommunicatorPtr sendUsingMe) {

         std :: string errMsg;
         bool success;

         //parse the query
         const UseTemporaryAllocationBlock block {128 * 1024 * 1024};
         PDB_COUT << "Got the ExecuteQuery object" << std :: endl;
         Handle <Vector <Handle<QueryBase>>> userQuery = sendUsingMe->getNextObject<Vector<Handle<QueryBase>>> (success, errMsg);
         if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return std :: make_pair (false, errMsg);
         }
         
         PDB_COUT << "To transform the ExecuteQuery object into a logicalGraph" << std :: endl;
         pdb_detail::QueryGraphIrPtr queryGraph = pdb_detail::buildIr(userQuery);

         PDB_COUT << "To transform the logicalGraph into a physical plan" << std :: endl;
         
         getFunctionality<QuerySchedulerServer>().parseOptimizedQuery(queryGraph);

#ifdef CLEAR_SET
         //So far we only clear for the first stage. (we now only schedule the first stage)
         Handle<SetIdentifier> output = getFunctionality<QuerySchedulerServer>().getOutputSet(); 
         std :: string outputTypeName = getFunctionality<QuerySchedulerServer>().getOutputTypeName();
         //check whether output exists, if yes, we remove that set and create a new set
         DistributedStorageManagerClient dsmClient(this->port, "localhost", logger);
         std :: cout << "QuerySchedulerServer: to clear output set with databaseName=" << output->getDatabase() << " and setName=" << output->getSetName() << " and typeName=" << outputTypeName << std :: endl;
         std :: cout << "Please turn CLEAR_SET flag off if client is responsible for creating output set" << std :: endl;
         bool ret = dsmClient.clearSet(output->getDatabase(), output->getSetName(), outputTypeName, errMsg);
         if (ret == false) {
              std :: cout << "QuerySchedulerServer: can't clear output set with databaseName=" << output->getDatabase() << " and setName=" << output->getSetName() << " and typeName=" << outputTypeName << std :: endl;
              return std :: make_pair (false, errMsg);
         }
         std :: cout << "QuerySchedulerServer: set cleared" << std :: endl;
#endif
         getFunctionality<QuerySchedulerServer>().printCurrentPlan();
         PDB_COUT << "To get the resource object from the resource manager" << std :: endl;
         getFunctionality<QuerySchedulerServer>().initialize(true);
         PDB_COUT << "To schedule the query to run on the cluster" << std :: endl;
         getFunctionality<QuerySchedulerServer>().schedule(); 
         PDB_COUT << "To send back response to client" << std :: endl; 
         Handle <SimpleRequestResult> result = makeObject <SimpleRequestResult> (true, std :: string ("successfully executed query"));
         if (!sendUsingMe->sendObject (result, errMsg)) {
              return std :: make_pair (false, errMsg);
         }
         PDB_COUT << "to cleanup" << std :: endl;
         getFunctionality<QuerySchedulerServer>().cleanup();
         return std :: make_pair (true, errMsg);

    

      }));
    
}


}



#endif
