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
#include "ShuffleInfo.h"
#include "ResourceManagerServer.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "InterfaceFunctions.h"
#include "QueryBase.h"
#include "PDBVector.h"
#include "Handle.h"
#include "ExecuteQuery.h"
#include "TupleSetExecuteQuery.h"
#include "ExecuteComputation.h"
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
#include "ClusterAggregateComp.h"
#include "QueryGraphAnalyzer.h"
#include "TCAPAnalyzer.h"
#include "Configuration.h"
#include "StorageCollectStats.h"
#include "StorageCollectStatsResponse.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

namespace pdb {

QuerySchedulerServer :: ~QuerySchedulerServer () {
    pthread_mutex_destroy(&connection_mutex);
}

QuerySchedulerServer :: QuerySchedulerServer(PDBLoggerPtr logger, bool pseudoClusterMode, double partitionToCoreRatio, bool isDynamicPlanning, bool removeIntermediateDataEarly) {
    this->port =8108;
    this->logger = logger;
    this->pseudoClusterMode = pseudoClusterMode;
    pthread_mutex_init(&connection_mutex, nullptr);
    this->jobStageId = 0;
    this->partitionToCoreRatio = partitionToCoreRatio;
    this->dynamicPlanningOrNot = isDynamicPlanning;
    this->earlyRemovingDataOrNot = removeIntermediateDataEarly;
    this->statsForOptimization = nullptr;
    this->initializeStats();
}


QuerySchedulerServer :: QuerySchedulerServer(int port, PDBLoggerPtr logger, bool pseudoClusterMode, double partitionToCoreRatio, bool isDynamicPlanning, bool removeIntermediateDataEarly) {
    this->port =port;
    this->logger = logger;
    this->pseudoClusterMode = pseudoClusterMode;
    pthread_mutex_init(&connection_mutex, nullptr);
    this->jobStageId = 0;
    this->partitionToCoreRatio = partitionToCoreRatio;
    this->dynamicPlanningOrNot = isDynamicPlanning;
    this->earlyRemovingDataOrNot = removeIntermediateDataEarly;
    this->statsForOptimization = nullptr;
    this->initializeStats();
}

void QuerySchedulerServer ::cleanup() {

    delete this->standardResources;
    this->standardResources = nullptr;

    for (int i = 0; i < currentPlan.size(); i++) {
             currentPlan[i]=nullptr;
    }
    this->currentPlan.clear();

    for (int i = 0; i < queryPlan.size(); i++) {
             queryPlan[i]=nullptr;
    }
    this->queryPlan.clear();

    for (int i = 0; i < interGlobalSets.size(); i++) {
             interGlobalSets[i]=nullptr;
    }
    this->interGlobalSets.clear();

    this->jobStageId = 0;
}

QuerySchedulerServer :: QuerySchedulerServer (std :: string resourceManagerIp, int port, PDBLoggerPtr logger, bool usePipelineNetwork, double partitionToCoreRatio, bool isDynamicPlanning, bool removeIntermediateDataEarly) {

     this->resourceManagerIp = resourceManagerIp;
     this->port = port;
     this->standardResources = nullptr;
     this->logger = logger;
     this->usePipelineNetwork = usePipelineNetwork;
     this->jobStageId = 0;
     this->partitionToCoreRatio = partitionToCoreRatio;
     this->dynamicPlanningOrNot = isDynamicPlanning;
     this->earlyRemovingDataOrNot = removeIntermediateDataEarly;
     this->statsForOptimization = nullptr;
     this->initializeStats();
}

void QuerySchedulerServer :: initialize(bool isRMRunAsServer) {
     if (this->standardResources != nullptr) {
         delete this->standardResources;
     }
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

             std :: cout << i << ": address=" << (*(resourceObjects))[i]->getAddress() << ", port="<< (*(resourceObjects))[i]->getPort() <<", node="<<(*(resourceObjects))[i]->getNodeId() <<", numCores=" << (*(resourceObjects))[i]->getNumCores() << ", memSize=" << (*(resourceObjects))[i]->getMemSize() << std :: endl;
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
             StandardResourceInfoPtr currentResource = std :: make_shared<StandardResourceInfo>(8/(nodeObjects->size()), (6*1024*1024)/(nodeObjects->size()), (*(nodeObjects))[i]->getAddress().c_str(), (*(nodeObjects))[i]->getPort(), (*(nodeObjects))[i]->getNodeId());
             this->standardResources->push_back(currentResource);
         }         
    }

}

//collect the statistics that will be used for optimizer
//this needs the functionality of catalog and distributed storage manager
void QuerySchedulerServer :: initializeStats () {
    //TODO: to load stats from file
    this->statsForOptimization = nullptr;
    this->standardResources = nullptr;
    return;
}

//return statsForOptimization
StatisticsPtr QuerySchedulerServer :: getStats () {
    return statsForOptimization;
}

//to schedule dynamic pipeline stages
//this must be invoked after initialize() and before cleanup()
void QuerySchedulerServer :: scheduleStages (std :: vector <Handle<AbstractJobStage>> & stagesToSchedule, std :: vector <Handle<SetIdentifier>> & intermediateSets, std :: shared_ptr<ShuffleInfo> shuffleInfo) {

         int counter = 0;
         PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer> (
             [&] (PDBAlarm myAlarm, int &counter) {
              counter ++;
              PDB_COUT << "counter = " << counter << std :: endl;
         }); 

         int numStages = stagesToSchedule.size();

         for (int i = 0; i < numStages; i ++) {
             for (int j = 0; j < shuffleInfo->getNumNodes(); j++) {
                 PDBWorkerPtr myWorker = getWorker();
                 PDBWorkPtr myWork = make_shared <GenericWork> (
                     [&, i, j] (PDBBuzzerPtr callerBuzzer) {
                          const UseTemporaryAllocationBlock block(4 * 1024 * 1024);

                          PDB_COUT << "to schedule the " << i << "-th stage on the " << j << "-th node" << std :: endl;

                          int port = (*(this->standardResources))[j]->getPort();
                          PDB_COUT << "port:" << port << std :: endl;
                          std :: string ip = (*(this->standardResources))[j]->getAddress();
                          PDB_COUT << "ip:" << ip  << std :: endl;
                          size_t memory = (*(this->standardResources))[j]->getMemSize();
                          //std :: cout << "Memory size for node-" << j << " :" << memory << std :: endl;
                          //create PDBCommunicator
                          pthread_mutex_lock(&connection_mutex);
                          PDB_COUT << "to connect to the remote node" << std :: endl;
                          PDBCommunicatorPtr communicator = std :: make_shared<PDBCommunicator>();

                          string errMsg;
                          bool success;
                          if(communicator->connectToInternetServer(logger, port, ip, errMsg)) {
                              success = false;
                              std :: cout << errMsg << std :: endl;
                              pthread_mutex_unlock(&connection_mutex);
                              callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                              return;
                          }
                          pthread_mutex_unlock(&connection_mutex);

                          //get current stage to schedule
                          Handle<AbstractJobStage> stage = stagesToSchedule[i];

                          //schedule the stage
                          if(stage->getJobStageType() == "TupleSetJobStage") {
                              Handle<TupleSetJobStage> tupleSetStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(stage);
                              tupleSetStage->setTotalMemoryOnThisNode(memory);
                              success = scheduleStage (j, tupleSetStage, communicator, DeepCopy);
                          } else if (stage->getJobStageType() == "AggregationJobStage" ) {
                              Handle<AggregationJobStage> aggStage = unsafeCast <AggregationJobStage, AbstractJobStage>(stage);
                              int numPartitionsOnThisNode = (int)((double)(standardResources->at(j)->getNumCores())*partitionToCoreRatio);
                              if(numPartitionsOnThisNode == 0) {
                                  numPartitionsOnThisNode = 1;
                              }
                              aggStage->setNumNodePartitions (numPartitionsOnThisNode);
                              aggStage->setAggTotalPartitions (shuffleInfo->getNumHashPartitions());
                              aggStage->setAggBatchSize(DEFAULT_BATCH_SIZE);
                              aggStage->setTotalMemoryOnThisNode(memory);
                              success = scheduleStage (j, aggStage, communicator, DeepCopy);
                          } else if (stage->getJobStageType() == "BroadcastJoinBuildHTJobStage" ) {
                              Handle<BroadcastJoinBuildHTJobStage> broadcastJoinStage = unsafeCast<BroadcastJoinBuildHTJobStage, AbstractJobStage> (stage);
                              success = scheduleStage (j, broadcastJoinStage, communicator, DeepCopy);

                          } else {
                              errMsg = "Unrecognized job stage";
                              std :: cout << errMsg << std :: endl;
                              success = false;
                          }
                          if (success == false) {
                              errMsg = std :: string ("Can't execute the ") + std :: to_string(i) + std :: string ("-th stage on the ") + std :: to_string(j) + std :: string ("-th node");
                              callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                              return;
                          }
                          callerBuzzer->buzz (PDBAlarm :: WorkAllDone, counter);
                     }
                 );
                 myWorker->execute(myWork, tempBuzzer);
             }
             while(counter < shuffleInfo->getNumNodes()) {
                tempBuzzer->wait();
             }
             counter = 0;
         }

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


//JiaNote TODO: consolidate below three functions into a template function
//to replace: schedule(Handle<JobStage>& stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode)
bool QuerySchedulerServer :: scheduleStage(int index, Handle<TupleSetJobStage>& stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode) {
        bool success;
        std :: string errMsg;
        PDB_COUT << "to send the job stage with id=" << stage->getStageId() << " to the "<< index <<"-th remote node" << std :: endl;

        if (mode == DeepCopy) {
             const UseTemporaryAllocationBlock block(4 * 1024 * 1024);              
             Handle<TupleSetJobStage> stageToSend = deepCopyToCurrentAllocationBlock<TupleSetJobStage> (stage);
             stageToSend->setNumNodes(this->shuffleInfo->getNumNodes());
             stageToSend->setNumTotalPartitions(this->shuffleInfo->getNumHashPartitions());
             Handle<Vector<Handle<Vector<HashPartitionID>>>> partitionIds = 
                              makeObject<Vector<Handle<Vector<HashPartitionID>>>> ();
             std :: vector < std :: vector < HashPartitionID>> standardPartitionIds = shuffleInfo->getPartitionIds();
             for (unsigned int i = 0; i < standardPartitionIds.size(); i++) {
                 Handle<Vector<HashPartitionID>> nodePartitionIds = makeObject <Vector<HashPartitionID>> ();
                 for (unsigned int j = 0; j < standardPartitionIds[i].size(); j++) {
                     nodePartitionIds->push_back( standardPartitionIds[i][j] );
                 }
                 partitionIds->push_back(nodePartitionIds);
             }
             stageToSend->setNumPartitions(partitionIds);

             Handle<Vector<String>> addresses = makeObject<Vector<String>> ();
             std :: vector < std :: string > standardAddresses = shuffleInfo->getAddresses();
             for (unsigned int i = 0; i < standardAddresses.size(); i++) {
                addresses->push_back(String(standardAddresses[i]));
             }
             stageToSend->setIPAddresses(addresses);
             stageToSend->setNodeId(index);
             success = communicator->sendObject<TupleSetJobStage>(stageToSend, errMsg);
             if (!success) {
                     std :: cout << errMsg << std :: endl;
                     return false;
             }
        } else {
             std :: cout << "Error: No such object creation mode supported in query scheduler" << std :: endl;
             return false;
        }
        PDB_COUT << "to receive query response from the "<< index << "-th remote node" << std :: endl;
        Handle<SetIdentifier> result = communicator->getNextObject<SetIdentifier>(success, errMsg);
        if (result != nullptr) {
             this->updateStats(result);
             PDB_COUT << "TupleSetJobStage execute: wrote set:" << result->getDatabase() << ":" << result->getSetName() << std :: endl;
        }
        else {
            PDB_COUT << "TupleSetJobStage execute failure: can't get results" << std :: endl;
            return false;
        }

        return true;
}

bool QuerySchedulerServer :: scheduleStage(int index, Handle<BroadcastJoinBuildHTJobStage>& stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode) {
        bool success;
        std :: string errMsg;
        PDB_COUT << "to send the job stage with id=" << stage->getStageId() << " to the "<< index <<"-th remote node" << std :: endl;

        if (mode == Direct) {
            success = communicator->sendObject<BroadcastJoinBuildHTJobStage>(stage, errMsg);
            if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return false;
            }
        }else if (mode == DeepCopy) {
             Handle<BroadcastJoinBuildHTJobStage> stageToSend = deepCopyToCurrentAllocationBlock<BroadcastJoinBuildHTJobStage> (stage);
             stageToSend->nullifyComputePlanPointer ();
             success = communicator->sendObject<BroadcastJoinBuildHTJobStage>(stageToSend, errMsg);
             if (!success) {
                     std :: cout << errMsg << std :: endl;
                     return false;
             }
        } else {
             std :: cout << "Error: No such object creation mode supported in query scheduler" << std :: endl;
             return false;
        }
        PDB_COUT << "to receive query response from the "<< index << "-th remote node" << std :: endl;
        Handle<SetIdentifier> result = communicator->getNextObject<SetIdentifier>(success, errMsg);
        if (result != nullptr) {
             this->updateStats(result);
             PDB_COUT << "BroadcastJoinBuildHTJobStage execute: wrote set:" << result->getDatabase() << ":" << result->getSetName()  << std :: endl;
        }
        else {
            PDB_COUT << "BroadcastJoinBuildHTJobStage execute failure: can't get results" << std :: endl;
            return false;
        }

        return true;
}

bool QuerySchedulerServer :: scheduleStage(int index, Handle<AggregationJobStage>& stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode) {
        bool success; 
        std :: string errMsg;
        PDB_COUT << "to send the job stage with id=" << stage->getStageId() << " to the "<< index << "-th remote node" << std :: endl;
        
        if (mode == Direct) {
            success = communicator->sendObject<AggregationJobStage>(stage, errMsg);
            if (!success) {
                 std :: cout << errMsg << std :: endl;
                 return false;
            } 
        }else if (mode == DeepCopy) { 
             Handle<AggregationJobStage> stageToSend = deepCopyToCurrentAllocationBlock<AggregationJobStage> (stage);
             success = communicator->sendObject<AggregationJobStage>(stageToSend, errMsg);
             if (!success) {
                     std :: cout << errMsg << std :: endl;
                     return false;
             } 
        } else { 
             std :: cout << "Error: No such object creation mode supported in query scheduler" << std :: endl;
             return false;
        }
        PDB_COUT << "to receive query response from the "<< index <<"-th remote node" << std :: endl;
        Handle<SetIdentifier> result = communicator->getNextObject<SetIdentifier>(success, errMsg);
        if (result != nullptr) {
             this->updateStats(result);
             PDB_COUT << "AggregationJobStage execute: wrote set:" << result->getDatabase() << ":" << result->getSetName() << std :: endl;
        }
        else {
            PDB_COUT << "AggregationJobStage execute failure: can't get results" << std :: endl;
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
             success = communicator->sendObject<JobStage>(stageToSend, errMsg);
             if (!success) {
                     std :: cout << errMsg << std :: endl;
                     return false;
             }
        } else if (mode == DeepCopy) {
             Handle<JobStage> stageToSend = deepCopyToCurrentAllocationBlock<JobStage> (stage);
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



bool QuerySchedulerServer :: parseTCAPString(Handle<Vector<Handle<Computation>>> myComputations, std :: string myTCAPString) {
    TCAPAnalyzer tcapAnalyzer(this->jobId, myComputations, myTCAPString, this->logger, false);
    return tcapAnalyzer.analyze(this->queryPlan, this->interGlobalSets);
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
//this must be invoked after initialize() and before cleanup()
void QuerySchedulerServer :: scheduleQuery() {
 
         //query plan
         int numStages = this->queryPlan.size();

         if (numStages > 1) {
            PDB_COUT << "#####################################" << std :: endl;
            PDB_COUT << "WARNING: GraphIr generates "<< numStages <<" stages" << std :: endl;
            PDB_COUT << "#####################################" << std :: endl;
         }

         std :: shared_ptr <ShuffleInfo> shuffleInfo = std :: make_shared <ShuffleInfo> (this->standardResources, this->partitionToCoreRatio);
         scheduleStages (this->queryPlan, this->interGlobalSets, shuffleInfo);

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

void QuerySchedulerServer :: collectStats () {
         std :: cout << "to collect stats" << std :: endl;
         this->statsForOptimization = make_shared<Statistics>();
         int counter = 0;
         PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer> (
             [&] (PDBAlarm myAlarm, int &counter) {
              counter ++;
              PDB_COUT << "counter = " << counter << std :: endl;
         });
         if (this->standardResources == nullptr) {
             initialize(true);
         }
         for (int i = 0; i < this->standardResources->size(); i++) {
                 PDBWorkerPtr myWorker = getWorker();
                 PDBWorkPtr myWork = make_shared <GenericWork> (
                     [&, i] (PDBBuzzerPtr callerBuzzer) {
                          const UseTemporaryAllocationBlock block(4 * 1024 * 1024);

                          PDB_COUT << "to collect stats on the " << i << "-th node" << std :: endl;
                          int port = (*(this->standardResources))[i]->getPort();
                          PDB_COUT << "port:" << port << std :: endl;
                          std :: string ip = (*(this->standardResources))[i]->getAddress();
                          PDB_COUT << "ip:" << ip  << std :: endl;

                          //create PDBCommunicator
                          pthread_mutex_lock(&connection_mutex);
                          PDB_COUT << "to connect to the remote node" << std :: endl;
                          PDBCommunicatorPtr communicator = std :: make_shared<PDBCommunicator>();

                          string errMsg;
                          bool success;
                          if(communicator->connectToInternetServer(logger, port, ip, errMsg)) {
                              success = false;
                              std :: cout << errMsg << std :: endl;
                              pthread_mutex_unlock(&connection_mutex);
                              callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                              return;
                          }
                          pthread_mutex_unlock(&connection_mutex);

                          //send StorageCollectStats to remote server 
                          Handle<StorageCollectStats> collectStatsMsg = makeObject<StorageCollectStats> ();
                          success = communicator->sendObject<StorageCollectStats>(collectStatsMsg, errMsg);
                          if (!success) {
                              std :: cout << errMsg << std :: endl;
                              callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                              return;
                          }
                          //receive StorageCollectStatsResponse from remote server
                          PDB_COUT << "to receive response from the "<< i << "-th remote node" << std :: endl;
                          Handle<StorageCollectStatsResponse> result = communicator->getNextObject<StorageCollectStatsResponse>(success, errMsg);
                          if (result != nullptr) {
                              //update stats
                              Handle<Vector<Handle<SetIdentifier>>> stats = result->getStats();
                              if (statsForOptimization == nullptr) {
                                   statsForOptimization = make_shared<Statistics>();
                              }
                              for (int j = 0; j < stats->size(); j++) {
                                   Handle<SetIdentifier> setToUpdateStats = (*stats)[j];
                                   this->updateStats (setToUpdateStats);
                              }

                          }   
                          else {
                               errMsg = "Collect response execute failure: can't get results";
                               std :: cout << errMsg << std :: endl;
                               callerBuzzer->buzz (PDBAlarm :: GenericError, counter);
                               return;
                          }   

                          if (success == false) {
                              errMsg = std :: string ("Can't collect stats from node with id=") + std :: to_string(i) + std :: string (" and ip=") + ip;
                              std :: cout << errMsg << std :: endl;
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
         counter = 0;
}

void QuerySchedulerServer :: updateStats (Handle<SetIdentifier> setToUpdateStats) {

        std :: string databaseName = setToUpdateStats->getDatabase();
        std :: string setName = setToUpdateStats->getSetName();
        size_t numPages = setToUpdateStats->getNumPages();
        size_t numOldPages = statsForOptimization->getNumPages(databaseName, setName);
        statsForOptimization->setNumPages(databaseName, setName, numPages+numOldPages);
        size_t pageSize = setToUpdateStats->getPageSize();
        statsForOptimization->setPageSize(databaseName, setName, pageSize);
        size_t numBytes = numPages*pageSize;
        size_t numOldBytes = statsForOptimization->getNumBytes(databaseName, setName);
        statsForOptimization->setNumBytes(databaseName, setName, numBytes+numOldBytes);

}

void QuerySchedulerServer :: registerHandlers (PDBServer &forMe) {

   //handler to schedule a Computation-based query graph
   forMe.registerHandler (ExecuteComputation_TYPEID, make_shared<SimpleRequestHandler<ExecuteComputation>> (

       [&] (Handle<ExecuteComputation> request, PDBCommunicatorPtr sendUsingMe) {

           std :: string errMsg;
           bool success;

           //parse the query
           const UseTemporaryAllocationBlock block { 128 * 1024 * 1024 };
           PDB_COUT << "Got the ExecuteComputation object" << std :: endl;
           Handle <Vector <Handle<Computation>>> computations = sendUsingMe->getNextObject<Vector<Handle<Computation>>> (success, errMsg);
           std :: string tcapString = request->getTCAPString();
           DistributedStorageManagerClient dsmClient(this->port, "localhost", logger);

           this->jobId = this->getNextJobId();
           //create the database first
           success = dsmClient.createDatabase(this->jobId, errMsg);


           if (success == true) {
              //we do not use dynamic planning
              if (this->dynamicPlanningOrNot == false) {
                  success = parseTCAPString(computations, tcapString); 
                  if (success == false) {
                      errMsg = "FATAL ERROR in QuerySchedulerServer: can't parse TCAP string.\n"+tcapString;
                  }
                  else {
                      //create intermediate sets:
                      PDB_COUT << "to create intermediate sets" << std :: endl;
                      if (success == true) {
                          for ( int i = 0; i < this->interGlobalSets.size(); i++ ) {
                              std :: string errMsg;
                              Handle<SetIdentifier> aggregationSet = this->interGlobalSets[i];
                              bool res = dsmClient.createTempSet(aggregationSet->getDatabase(), aggregationSet->getSetName(), "IntermediateSet", errMsg);
                              if (res != true) {
                                  std :: cout << "can't create temp set: " <<errMsg << std :: endl;
                              } else {
                                  PDB_COUT << "Created set with database=" << aggregationSet->getDatabase() << ", set=" << aggregationSet->getSetName() << std :: endl;
                              }
                          }

                          getFunctionality<QuerySchedulerServer>().printStages();
                          PDB_COUT << "To get the resource object from the resource manager" << std :: endl;
                          getFunctionality<QuerySchedulerServer>().initialize(true);
                          PDB_COUT << "To schedule the query to run on the cluster" << std :: endl;
                          getFunctionality<QuerySchedulerServer>().scheduleQuery();

                     }

                 }

              } else {

                //analyze resources
                PDB_COUT << "To get the resource object from the resource manager" << std :: endl;
                getFunctionality<QuerySchedulerServer>().initialize(true);
                this->shuffleInfo = std :: make_shared <ShuffleInfo> (this->standardResources, this->partitionToCoreRatio);

                if(this->statsForOptimization == nullptr) {
                    this->collectStats();
                }

                
                //dyanmic planning
                //initialize tcapAnalyzer
                this->tcapAnalyzerPtr = make_shared<TCAPAnalyzer> (jobId, computations, tcapString, this->logger, true);
                int jobStageId = 0;
                while (this->tcapAnalyzerPtr->getNumSources() > 0) {

                    //analyze all sources and select a source based on cost model
                    int indexOfBestSource = this->tcapAnalyzerPtr->getBestSource(this->statsForOptimization);

                    //get the job stages and intermediate data sets for this source
                    std :: string sourceName = this->tcapAnalyzerPtr->getSourceSetName (indexOfBestSource);
                    Handle<SetIdentifier> sourceSet = this->tcapAnalyzerPtr->getSourceSetIdentifier (sourceName);
                    AtomicComputationPtr sourceAtomicComp = this->tcapAnalyzerPtr->getSourceComputation (sourceName);
                    std :: vector<Handle<AbstractJobStage>> jobStages;
                    std :: vector<Handle<SetIdentifier>> intermediateSets;
                    this->tcapAnalyzerPtr->getNextStages(jobStages, intermediateSets, sourceAtomicComp, sourceSet, jobStageId);

                    //create intermediate sets
                    for ( int i = 0; i < intermediateSets.size(); i++ ) {
                        std :: string errMsg;
                        Handle<SetIdentifier> intermediateSet = intermediateSets[i];
                        bool res = dsmClient.createTempSet(intermediateSet->getDatabase(), intermediateSet->getSetName(), "IntermediateData", errMsg);
                        if (res != true) {
                            std :: cout << "can't create temp set: " <<errMsg << std :: endl;
                        } else {
                            PDB_COUT << "Created set with database=" << intermediateSet->getDatabase() << ", set=" << intermediateSet->getSetName() << std :: endl;
                        }
                    }
                    //schedule this job stages
                    PDB_COUT << "To schedule the query to run on the cluster" << std :: endl;
                    getFunctionality<QuerySchedulerServer>().scheduleStages(jobStages, intermediateSets, shuffleInfo);

                    
                    //to remember the intermediate sets:
                    PDB_COUT << "to remember intermediate sets" << std :: endl;
                    for ( int i = 0; i < intermediateSets.size(); i++ ) {
                        this->interGlobalSets.push_back(intermediateSets[i]);
                    }

                }
              
             }
             //to remove intermediate sets:
             PDB_COUT << "to remove intermediate sets" << std :: endl;
             for ( int i = 0; i < this->interGlobalSets.size(); i++ ) {
                 std :: string errMsg;
                 Handle<SetIdentifier> intermediateSet = this->interGlobalSets[i];
                 bool res = dsmClient.removeTempSet(intermediateSet->getDatabase(), intermediateSet->getSetName(), "IntermediateData", errMsg);
                 if (res != true) {
                     std :: cout << "can't remove temp set: " <<errMsg << std :: endl;
                 } else {
                     PDB_COUT << "Removed set with database=" << intermediateSet->getDatabase() << ", set=" << intermediateSet->getSetName() << std :: endl;
                 }
             }
           }
           PDB_COUT << "To send back response to client" << std :: endl;
           Handle <SimpleRequestResult> result = makeObject <SimpleRequestResult> (success, errMsg);
           if (!sendUsingMe->sendObject (result, errMsg)) {
                 PDB_COUT << "to cleanup" << std :: endl;
                 getFunctionality<QuerySchedulerServer>().cleanup();
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
