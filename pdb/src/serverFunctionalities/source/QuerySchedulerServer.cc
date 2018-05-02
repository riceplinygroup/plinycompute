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


#include "PDBDebug.h"
#include "InterfaceFunctions.h"
#include "QuerySchedulerServer.h"
#include "ResourceManagerServer.h"
#include "SimpleRequestHandler.h"
#include "GenericWork.h"
#include "StorageCollectStats.h"
#include "StorageCollectStatsResponse.h"
#include "Profiling.h"
#include "../../optimizer/headers/PrologOptimizer.h"
#include <ctime>
#include <chrono>

namespace pdb {

QuerySchedulerServer::~QuerySchedulerServer() {
    pthread_mutex_destroy(&connection_mutex);
}

QuerySchedulerServer::QuerySchedulerServer(PDBLoggerPtr logger,
                                           ConfigurationPtr conf,
                                           bool pseudoClusterMode,
                                           double partitionToCoreRatio) {
    pthread_mutex_init(&connection_mutex, nullptr);

    this->port = 8108;
    this->logger = logger;
    this->conf = conf;
    this->pseudoClusterMode = pseudoClusterMode;
    this->partitionToCoreRatio = partitionToCoreRatio;
    this->statsForOptimization = nullptr;
    this->standardResources = nullptr;
}


QuerySchedulerServer::QuerySchedulerServer(int port,
                                           PDBLoggerPtr logger,
                                           ConfigurationPtr conf,
                                           bool pseudoClusterMode,
                                           double partitionToCoreRatio) {
    pthread_mutex_init(&connection_mutex, nullptr);

    this->port = port;
    this->logger = logger;
    this->conf = conf;
    this->pseudoClusterMode = pseudoClusterMode;
    this->partitionToCoreRatio = partitionToCoreRatio;
    this->statsForOptimization = nullptr;
    this->standardResources = nullptr;
}

void QuerySchedulerServer::cleanup() {

    // delete standard resources if they exist and set them to null
    delete this->standardResources;
    this->standardResources = nullptr;

    // clean the list of intermediate sets that need to be removed
    for (auto &interGlobalSet : interGlobalSets) {
        interGlobalSet = nullptr;
    }
    this->interGlobalSets.clear();
}

void QuerySchedulerServer::initialize() {

  // remove the standard resources if there are any
  delete this->standardResources;
  this->standardResources = new std::vector<StandardResourceInfoPtr>();

  // depending of whether we are running in pseudo cluster mode
  // or not we need to grab the standard resources from a different place
  if (!pseudoClusterMode) {
      initializeForServerMode();
  } else {
      initializeForPseudoClusterMode();
  }
}

void QuerySchedulerServer::initializeForPseudoClusterMode() {

    // all the stuff we create will be stored here
    const UseTemporaryAllocationBlock block(2 * 1024 * 1024);

    PDB_COUT << "To get the node object from the resource manager" << std::endl;
    auto nodeObjects = getFunctionality<ResourceManagerServer>().getAllNodes();

    // add and print out the resources
    for (int i = 0; i < nodeObjects->size(); i++) {

        PDB_COUT << i << ": address=" << (*(nodeObjects))[i]->getAddress()
                 << ", port=" << (*(nodeObjects))[i]->getPort()
                 << ", node=" << (*(nodeObjects))[i]->getNodeId() << std::endl;
        StandardResourceInfoPtr currentResource =
                std::make_shared<StandardResourceInfo>(DEFAULT_NUM_CORES / (nodeObjects->size()),
                                                       DEFAULT_MEM_SIZE / (nodeObjects->size()),
                                                       (*(nodeObjects))[i]->getAddress().c_str(),
                                                       (*(nodeObjects))[i]->getPort(),
                                                       (*(nodeObjects))[i]->getNodeId());
        this->standardResources->push_back(currentResource);
    }
}

void QuerySchedulerServer::initializeForServerMode() {

    // all the stuff we create will be stored here
    const UseTemporaryAllocationBlock block(2 * 1024 * 1024);

    PDB_COUT << "To get the resource object from the resource manager" << std::endl;
    auto resourceObjects = getFunctionality<ResourceManagerServer>().getAllResources();

    // add and print out the resources
    for (int i = 0; i < resourceObjects->size(); i++) {

        PDB_COUT << i << ": address=" << (*(resourceObjects))[i]->getAddress()
                 << ", port=" << (*(resourceObjects))[i]->getPort()
                 << ", node=" << (*(resourceObjects))[i]->getNodeId()
                 << ", numCores=" << (*(resourceObjects))[i]->getNumCores()
                 << ", memSize=" << (*(resourceObjects))[i]->getMemSize() << std::endl;
        StandardResourceInfoPtr currentResource = std::make_shared<StandardResourceInfo>(
                (*(resourceObjects))[i]->getNumCores(),
                (*(resourceObjects))[i]->getMemSize(),
                (*(resourceObjects))[i]->getAddress().c_str(),
                (*(resourceObjects))[i]->getPort(),
                (*(resourceObjects))[i]->getNodeId());
        this->standardResources->push_back(currentResource);
    }
}


StatisticsPtr QuerySchedulerServer::getStats() {
    return statsForOptimization;
}


void QuerySchedulerServer::scheduleStages(std::vector<Handle<AbstractJobStage>>& stagesToSchedule,
                                          std::shared_ptr<ShuffleInfo> shuffleInfo) {

    int counter = 0;

    // create the buzzer
    PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>([&](PDBAlarm myAlarm, int& cnt) {
        cnt++;
        PDB_COUT << "counter = " << cnt << std::endl;
    });

    // go though all the stages and send them to every node
    for (auto &stage : stagesToSchedule) {
        for (unsigned long node = 0; node < shuffleInfo->getNumNodes(); node++) {

            // grab a worker
            PDBWorkerPtr myWorker = getWorker();

            // create some work for it
            PDBWorkPtr myWork = make_shared<GenericWork>([&, node](PDBBuzzerPtr callerBuzzer) {
                prepareAndScheduleStage(stage, node, counter, callerBuzzer);
            });

            // execute the work
            myWorker->execute(myWork, tempBuzzer);
        }

        // wait until all the nodes are finished
        while (counter < shuffleInfo->getNumNodes()) {
            tempBuzzer->wait();
        }

        // reset the counter for the next stage
        counter = 0;
    }
}

void QuerySchedulerServer::prepareAndScheduleStage(Handle<AbstractJobStage> &stage,
                                                   unsigned long node,
                                                   int &counter,
                                                   PDBBuzzerPtr &callerBuzzer){
    // this is where all the stuff we create will be stored (the deep copy of the stage)
    const UseTemporaryAllocationBlock block(256 * 1024 * 1024);

    // grab the port and the address of the node node from the standard resources
    int port = this->standardResources->at(node)->getPort();
    std::string ip = this->standardResources->at(node)->getAddress();

    PROFILER_START(scheduleStage)

    // create PDBCommunicator
    PDBCommunicatorPtr communicator = getCommunicatorToNode(port, ip);

    // if we failed to acquire a communicator to the node signal a failure and finish
    if(communicator == nullptr) {
        callerBuzzer->buzz(PDBAlarm::GenericError, counter);
        return;
    }

    // figure out what kind of stage it is and schedule it
    bool success;
    switch (stage->getJobStageTypeID()) {
        case TupleSetJobStage_TYPEID : {
            Handle<TupleSetJobStage> tupleSetStage = unsafeCast<TupleSetJobStage, AbstractJobStage>(stage);
            success = scheduleStage(node, tupleSetStage, communicator);
            break;
        }
        case AggregationJobStage_TYPEID : {
            Handle<AggregationJobStage> aggStage = unsafeCast<AggregationJobStage, AbstractJobStage>(stage);

            // TODO this is bad, concurrent modification need to move it to the right place!
            aggStage->setAggTotalPartitions(shuffleInfo->getNumHashPartitions());
            aggStage->setAggBatchSize(DEFAULT_BATCH_SIZE);
            success = scheduleStage(node, aggStage, communicator);
            break;
        }
        case BroadcastJoinBuildHTJobStage_TYPEID : {
            Handle<BroadcastJoinBuildHTJobStage> broadcastJoinStage =
                    unsafeCast<BroadcastJoinBuildHTJobStage, AbstractJobStage>(stage);
            success = scheduleStage(node, broadcastJoinStage, communicator);
            break;
        }
        case HashPartitionedJoinBuildHTJobStage_TYPEID : {
            Handle<HashPartitionedJoinBuildHTJobStage> hashPartitionedJoinStage =
                    unsafeCast<HashPartitionedJoinBuildHTJobStage, AbstractJobStage>(stage);
            success = scheduleStage(node, hashPartitionedJoinStage, communicator);
            break;
        }
        default: {
            PDB_COUT << "Unrecognized job stage" << std::endl;
            success = false;
            break;
        }
    }

    PROFILER_END_MESSAGE(scheduleStage, "For stage : " << stage->getStageId() << " on node" << ip)

    // if we failed to execute the stage on the node node we signal a failure
    if (!success) {
        PDB_COUT << "Can't execute the " << stage->getJobStageType() << " with " << stage->getStageId()
                 << " on the " << std::to_string(node) << "-th node" << std::endl;
        callerBuzzer->buzz(PDBAlarm::GenericError, counter);
        return;
    }

    // excellent everything worked just as expected
    callerBuzzer->buzz(PDBAlarm::WorkAllDone, counter);
}

PDBCommunicatorPtr QuerySchedulerServer::getCommunicatorToNode(int port, std::string &ip) {

    // lock the connection mutex so no other thread tries to connect to a node
    pthread_mutex_lock(&connection_mutex);
    PDBCommunicatorPtr communicator = std::make_shared<PDBCommunicator>();

    // log what we are doing
    PDB_COUT << "Connecting to remote node connect to the remote node with address : " << ip  << ":" << port << std::endl;

    // try to connect to the node
    string errMsg;
    bool failure = communicator->connectToInternetServer(logger, port, ip, errMsg);

    // we are don connecting somebody else can do it now
    pthread_mutex_unlock(&connection_mutex);

    // if we succeeded return the communicator
    if (!failure) {
        return communicator;
    }

    // otherwise log the error message
    std::cout << errMsg << std::endl;

    // return a null pointer
    return nullptr;
}


template<typename T>
bool QuerySchedulerServer::scheduleStage(unsigned long node,
                                         Handle<T>& stage,
                                         PDBCommunicatorPtr communicator){
    bool success;
    std::string errMsg;
    PDB_COUT << "to send the job stage with id="
             << stage->getStageId() << " to the " << node << "-th remote node" << std::endl;

    // the copy we are about to make will be stored here
    const UseTemporaryAllocationBlock block(256 * 1024 * 1024);

    // get a copy of the stage, that is prepared to be sent
    Handle<T> stageToSend = getStageToSend(node, stage);

    // send the stage to the execution server
    success = communicator->sendObject<T>(stageToSend, errMsg);

    // check if we succeeded on doing that if not we have a problem
    if (!success) {
        std::cout << errMsg << std::endl;
        return false;
    }

    PDB_COUT << "to receive query response from the " << node << "-th remote node" << std::endl;
    Handle<SetIdentifier> result = communicator->getNextObject<SetIdentifier>(success, errMsg);

    // check if we succeeded in executing the stage
    if(result == nullptr) {
        PDB_COUT << stage->getJobStageType() << "TupleSetJobStage execute failure: can't get results" << std::endl;
        return false;
    }

    // update the statistics based on the returned results
    this->updateStats(result);
    PDB_COUT << stage->getJobStageType() << " execute: wrote set:" << result->getDatabase()
             << ":" << result->getSetName() << std::endl;

    return true;
}

Handle<TupleSetJobStage> QuerySchedulerServer::getStageToSend(unsigned long index,
                                                              Handle<TupleSetJobStage> &stage) {

    // do a deep copy of the stage
    Handle<TupleSetJobStage> stageToSend = deepCopyToCurrentAllocationBlock<TupleSetJobStage>(stage);

    // set the number of nodes and the number of hash partitions
    stageToSend->setNumNodes(this->shuffleInfo->getNumNodes());
    stageToSend->setNumTotalPartitions(this->shuffleInfo->getNumHashPartitions());

    // grab the partition IDs on each node
    std::vector<std::vector<HashPartitionID>> standardPartitionIds = shuffleInfo->getPartitionIds();

    // copy the IDs into a new vector of vectors
    Handle<Vector<Handle<Vector<HashPartitionID>>>> partitionIds = makeObject<Vector<Handle<Vector<HashPartitionID>>>>();
    for (auto &standardPartitionId : standardPartitionIds) {
        Handle<Vector<HashPartitionID>> nodePartitionIds = makeObject<Vector<HashPartitionID>>();
        for (unsigned int id : standardPartitionId) {
            nodePartitionIds->push_back(id);
        }
        partitionIds->push_back(nodePartitionIds);
    }

    // set the memory on the node we want to send it
    stageToSend->setTotalMemoryOnThisNode((size_t)(*(this->standardResources))[index]->getMemSize());

    // set the partition IDs for each node
    stageToSend->setNumPartitions(partitionIds);

    // grab the addresses for each node
    std::vector<std::string> standardAddresses = shuffleInfo->getAddresses();

    // go through each address and copy it into a vector of strings
    Handle<Vector<String>> addresses = makeObject<Vector<String>>();
    for (const auto &standardAddress : standardAddresses) {
        addresses->push_back(String(standardAddress));
    }

    // set the addresses of the nodes
    stageToSend->setIPAddresses(addresses);
    stageToSend->setNodeId(static_cast<NodeID>(index));

    return stageToSend;
}

Handle<AggregationJobStage> QuerySchedulerServer::getStageToSend(unsigned long index,
                                                                 Handle<AggregationJobStage> &stage) {

    // do a deep copy of the stage
    Handle<AggregationJobStage> stageToSend =
            deepCopyToCurrentAllocationBlock<AggregationJobStage>(stage);

    // figure out the number of partitions on the node we want to send it
    auto numPartitionsOnThisNode = (int)((double)(standardResources->at(index)->getNumCores()) * partitionToCoreRatio);
    if (numPartitionsOnThisNode == 0) {
        numPartitionsOnThisNode = 1;
    }

    // fill in the info about the node
    stageToSend->setNumNodePartitions(numPartitionsOnThisNode);
    stageToSend->setTotalMemoryOnThisNode((size_t)(*(this->standardResources))[index]->getMemSize());

    // TODO these two need to be relocated
    stageToSend->setAggTotalPartitions(shuffleInfo->getNumHashPartitions());
    stageToSend->setAggBatchSize(DEFAULT_BATCH_SIZE);

    return stageToSend;
}

Handle<BroadcastJoinBuildHTJobStage> QuerySchedulerServer::getStageToSend(unsigned long index,
                                                                          Handle<BroadcastJoinBuildHTJobStage> &stage) {

    // do a deep copy of the stage
    Handle<BroadcastJoinBuildHTJobStage> stageToSend =
            deepCopyToCurrentAllocationBlock<BroadcastJoinBuildHTJobStage>(stage);

    // set the reference to the compute plan to null so it's not sent
    stageToSend->nullifyComputePlanPointer();

    // set the memory on the node we want to send it
    stageToSend->setTotalMemoryOnThisNode((size_t)(*(this->standardResources))[index]->getMemSize());

    return stageToSend;
}

Handle<HashPartitionedJoinBuildHTJobStage> QuerySchedulerServer::getStageToSend(unsigned long index,
                                                                                Handle<HashPartitionedJoinBuildHTJobStage> &stage) {

    // do a deep copy of the stage
    Handle<HashPartitionedJoinBuildHTJobStage> stageToSend =
            deepCopyToCurrentAllocationBlock<HashPartitionedJoinBuildHTJobStage>(stage);

    // set the reference to the compute plan to null so it's not sent
    stageToSend->nullifyComputePlanPointer();

    // figure out the number of partitions on the node we want to send it
    auto numPartitionsOnThisNode = (int)((double)(standardResources->at(index)->getNumCores()) * partitionToCoreRatio);
    if (numPartitionsOnThisNode == 0) {
        numPartitionsOnThisNode = 1;
    }

    // set the value we just calculated
    stageToSend->setNumNodePartitions(numPartitionsOnThisNode);

    // set the memory on the node we want to send it
    stageToSend->setTotalMemoryOnThisNode((size_t)(*(this->standardResources))[index]->getMemSize());

    return stageToSend;
}

void QuerySchedulerServer::collectStats() {

    // we use this variable to sync all the nodes
    int counter = 0;

    // create the buzzer
    PDBBuzzerPtr tempBuzzer = make_shared<PDBBuzzer>([&](PDBAlarm myAlarm, int& cnt) {
        cnt++;
        PDB_COUT << "counter = " << cnt << std::endl;
    });

    // if the standard resources are for some reason not initialized initialize them
    if (this->standardResources == nullptr) {
        initialize();
    }

    this->statsForOptimization = make_shared<Statistics>();

    // go through each node
    for (int node = 0; node < this->standardResources->size(); node++) {

        // grab one worker
        PDBWorkerPtr myWorker = getWorker();

        // make some work to collect the stats for the current node
        PDBWorkPtr myWork = make_shared<GenericWork>([&, node](PDBBuzzerPtr callerBuzzer) {
            collectStatsForNode(node, counter, callerBuzzer);
        });

        // execute the work
        myWorker->execute(myWork, tempBuzzer);
    }

    // wait until everything is finished
    while (counter < this->standardResources->size()) {
        tempBuzzer->wait();
    }
}

void QuerySchedulerServer::collectStatsForNode(int node,
                                               int &counter,
                                               PDBBuzzerPtr &callerBuzzer) {

    bool success;
    std::string errMsg;

    // all the stuff that we create in this method will be stored here
    const UseTemporaryAllocationBlock block(4 * 1024 * 1024);

    // grab the port and the ip of the node
    int port = (*(this->standardResources))[node]->getPort();
    std::string ip = (*(this->standardResources))[node]->getAddress();

    // create PDBCommunicator
    PDBCommunicatorPtr communicator = getCommunicatorToNode(port, ip);

    // if we failed to acquire a communicator to the node signal a failure and finish
    if(communicator == nullptr) {
        callerBuzzer->buzz(PDBAlarm::GenericError, counter);
        return;
    }

    // make a request to remote server for the statistics
    PDB_COUT << "About to collect stats on the " << node << "-th node" << std::endl;
    requestStatistics(communicator, success, errMsg);

    // we failed to request print the reason for the failure and signal an error
    if (!success) {
        std::cout << errMsg << std::endl;
        callerBuzzer->buzz(PDBAlarm::GenericError, counter);
        return;
    }

    // receive StorageCollectStatsResponse from remote server
    PDB_COUT << "About to receive response from the " << node << "-th remote node" << std::endl;
    Handle<StorageCollectStatsResponse> result = communicator->getNextObject<StorageCollectStatsResponse>(success,
                                                                                                          errMsg);

    // we failed to receive the result, print out what happened and signal an error
    if (!success || result == nullptr) {
        PDB_COUT << "Can't get results from node with id=" << std::to_string(node) << " and ip=" << ip << std::endl;
        callerBuzzer->buzz(PDBAlarm::GenericError, counter);
        return;
    }

    // update stats
    Handle<Vector<Handle<SetIdentifier>>> stats = result->getStats();
    for (int j = 0; j < stats->size(); j++) {
        this->updateStats((*stats)[j]);
    }

    // lose the reference to the result
    result = nullptr;

    // great! we succeeded in collecting the statistics for this node signal that
    callerBuzzer->buzz(PDBAlarm::WorkAllDone, counter);
}

void QuerySchedulerServer::requestStatistics(PDBCommunicatorPtr &communicator, bool &success, string &errMsg) const {
    Handle<StorageCollectStats> collectStatsMsg = makeObject<StorageCollectStats>();
    success = communicator->sendObject<StorageCollectStats>(collectStatsMsg, errMsg);
}

void QuerySchedulerServer::updateStats(Handle<SetIdentifier> setToUpdateStats) {

    // grab the database name and the name of the set we are updating
    std::string databaseName = setToUpdateStats->getDatabase();
    std::string setName = setToUpdateStats->getSetName();

    // figure out the values we need tu updated
    size_t numPages = setToUpdateStats->getNumPages();
    size_t pageSize = setToUpdateStats->getPageSize();
    size_t numBytes = numPages * pageSize;

    // update the statistics
    statsForOptimization->setPageSize(databaseName, setName, pageSize);
    statsForOptimization->incrementNumPages(databaseName, setName, numPages);
    statsForOptimization->incrementNumBytes(databaseName, setName, numBytes);
}


void QuerySchedulerServer::registerHandlers(PDBServer& forMe) {

    // handler to schedule a Computation-based query graph
    forMe.registerHandler(
        ExecuteComputation_TYPEID,
        make_shared<SimpleRequestHandler<ExecuteComputation>>(
                [&](Handle<ExecuteComputation> request, PDBCommunicatorPtr sendUsingMe) {
                return executeComputation(request, sendUsingMe);
            }));
}


pair<bool, basic_string<char>> QuerySchedulerServer::executeComputation(Handle<ExecuteComputation> &request,
                                                                        PDBCommunicatorPtr &sendUsingMe) {
    // all the stuff will be allocated here
    const UseTemporaryAllocationBlock block{256 * 1024 * 1024};

    std::string errMsg;
    bool success;

    // parse the query
    PDB_COUT << "Got the ExecuteComputation object" << std::endl;
    Handle<Vector<Handle<Computation>>> computations = sendUsingMe->getNextObject<Vector<Handle<Computation>>>(success,
                                                                                                               errMsg);
    // we create a new jobID
    this->jobId = this->getNextJobId();

    // use that jobID to create a database for the job
    DistributedStorageManagerClient dsmClient(this->port, "localhost", logger);
    if(!dsmClient.createDatabase(this->jobId, errMsg)) {
        PDB_COUT << "Could not crate a database for " << this->jobId << ", cleaning up!" <<  std::endl;
        getFunctionality<QuerySchedulerServer>().cleanup();
        return std::make_pair(false, errMsg);
    }

    // initialize the standard resources from the resource manager
    PDB_COUT << "To get the resource object from the resource manager" << std::endl;
    getFunctionality<QuerySchedulerServer>().initialize();


    // create the shuffle info (just combine the standard resources with the partition to core ration) TODO ask Jia if this is really necessary
    this->shuffleInfo = std::make_shared<ShuffleInfo>(this->standardResources,
                                                      this->partitionToCoreRatio);

    // if we don't have the information about the sets we ask every node to submit them
    if (this->statsForOptimization == nullptr) {
        this->collectStats();
    }

    PDB_COUT << "TCAP before optimization: \n";
    PDB_COUT << "\033[1;31m" << request->getTCAPString() << "\033[0m";

    // create the TCAP optimizer
    TCAPOptimizerPtr optimizer = make_shared<PrologOptimizer>();

    // optimize the TCAP
    std::string optimizedTCAP = optimizer->optimize(request->getTCAPString());

    PDB_COUT << std::endl << "TCAP after optimization: \n" << std::endl;
    PDB_COUT << "\033[1;36m" << optimizedTCAP << "\033[0m";

    // initialize the tcapAnalyzer - used to generate the pipelines and pipeline stages we need to execute
    this->tcapAnalyzerPtr = make_shared<TCAPAnalyzer>(jobId,
                                                      this->logger,
                                                      this->conf,
                                                      optimizedTCAP,
                                                      computations);
    int jobStageId = 0;
    while (this->tcapAnalyzerPtr->hasSources()) {

        std::vector<Handle<AbstractJobStage>> jobStages;
        std::vector<Handle<SetIdentifier>> intermediateSets;

        /// do the physical planning
        PROFILER_START(physicalPlanning)

        extractPipelineStages(jobStageId, jobStages, intermediateSets);

        PROFILER_END(physicalPlanning)

        /// create intermediate sets
        PROFILER_START(createIntermediateSets)

        createIntermediateSets(dsmClient, intermediateSets);

        PROFILER_END(createIntermediateSets)

        /// schedule this job stages
        PROFILER_START(scheduleStages)

        PDB_COUT << "To schedule the query to run on the cluster" << std::endl;
        getFunctionality<QuerySchedulerServer>().scheduleStages(jobStages, shuffleInfo);

        PROFILER_END(scheduleStages)

        // removes the intermediate sets we don't anymore to continue the execution
        removeUnusedIntermediateSets(dsmClient, intermediateSets);
    }

    // removes the rest of the intermediate sets
    PDB_COUT << "About to remove intermediate sets" << endl;
    removeIntermediateSets(dsmClient);

    // notify the client that we succeeded
    PDB_COUT << "About to send back response to client" << std::endl;
    Handle<SimpleRequestResult> result = makeObject<SimpleRequestResult>(success, errMsg);

    if (!sendUsingMe->sendObject(result, errMsg)) {
        PDB_COUT << "About to cleanup" << std::endl;
        getFunctionality<QuerySchedulerServer>().cleanup();
        return std::make_pair(false, errMsg);
    }

    PDB_COUT << "About to cleanup" << std::endl;
    getFunctionality<QuerySchedulerServer>().cleanup();
    return std::make_pair(true, errMsg);
}

void QuerySchedulerServer::removeUnusedIntermediateSets(DistributedStorageManagerClient &dsmClient,
                                                        vector<Handle<SetIdentifier>> &intermediateSets) {

    // to remove the intermediate sets:
    for (const auto &intermediateSet : intermediateSets) {

        // check whether intermediateSet is a source set and has consumers
        string setName = intermediateSet->toSourceSetName();
        if (this->tcapAnalyzerPtr->hasConsumers(setName)) {

            // if it does then we need to remember this set and not remove it, because it will be used later
            this->interGlobalSets.push_back(intermediateSet);
            continue;
        }

        // check if we failed, if we did log it and continue to the next set
        std::string errMsg;
        bool res = dsmClient.removeTempSet(intermediateSet->getDatabase(),
                                        intermediateSet->getSetName(),
                                        "IntermediateData",
                                        errMsg);

        // check if we failed, if we did log it and continue to the next set
        if (!res) {
            std::cout << "can't remove temp set: " << errMsg << std::endl;
            continue;
        }

        // we succeeded in removing the set, log that
        std::cout << "Removed set with database=" << intermediateSet->getDatabase() << ", set="
                  << intermediateSet->getSetName() << std::endl;
    }
}

void QuerySchedulerServer::removeIntermediateSets(DistributedStorageManagerClient &dsmClient) {

    // go through the remaining intermediate sets and remove them
    for (const auto &intermediateSet : interGlobalSets) {

        // send a request to the DistributedStorageManagerClient to remove it
        string errMsg;
        bool res = dsmClient.removeTempSet(intermediateSet->getDatabase(),
                                           intermediateSet->getSetName(),
                                           "IntermediateData",
                                           errMsg);

        // check if we failed, if we did log it and continue to the next set
        if (!res) {
            cout << "Can not remove temp set: " << errMsg << endl;
            continue;
        }

        // we succeeded in removing the set, log that
        PDB_COUT << "Removed set with database=" << intermediateSet->getDatabase() << ", set="
                 << intermediateSet->getSetName() << endl;
    }
}

void QuerySchedulerServer::createIntermediateSets(DistributedStorageManagerClient &dsmClient,
                                                  vector<Handle<SetIdentifier>> &intermediateSets) {

    // go through each intermediate set list and create them
    for (const auto &intermediateSet : intermediateSets) {

        // send a request to the DistributedStorageManagerClient to remove it
        string errMsg;
        bool res = dsmClient.createTempSet(intermediateSet->getDatabase(),
                                           intermediateSet->getSetName(),
                                           "IntermediateData",
                                           errMsg,
                                           intermediateSet->getPageSize());

        // check if we failed, if we did log it and continue to the next set
        if (!res) {
            cout << "Can not create temp set: " << errMsg << endl;
            continue;
        }

        // great everything went well log that...
        PDB_COUT << "Created set with database=" << intermediateSet->getDatabase() << ", set="
                 << intermediateSet->getSetName() << endl;
    }
}

void QuerySchedulerServer:: extractPipelineStages(int &jobStageId,
                                                  vector<Handle<AbstractJobStage>> &jobStages,
                                                  vector<Handle<SetIdentifier>> &intermediateSets) {

    // try to get a sequence of stages, if we have any sources left
    bool success = false;
    while (this->tcapAnalyzerPtr->hasSources() && !success) {

        // get the next sequence of stages returns false if it selects the wrong source, and needs to retry it
        success = this->tcapAnalyzerPtr->getNextStagesOptimized(jobStages,
                                                                intermediateSets,
                                                                statsForOptimization,
                                                                jobStageId);
    }
}

}


#endif
