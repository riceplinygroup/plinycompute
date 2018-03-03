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
#ifndef QUERY_SCHEDULER_SERVER_H
#define QUERY_SCHEDULER_SERVER_H


#include "ServerFunctionality.h"
#include "ResourceInfo.h"
#include "StandardResourceInfo.h"
#include "Handle.h"
#include "PDBVector.h"
#include "QueryBase.h"
#include "ResourceInfo.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "PDBLogger.h"
#include "TupleSetJobStage.h"
#include "AggregationJobStage.h"
#include "BroadcastJoinBuildHTJobStage.h"
#include "HashPartitionedJoinBuildHTJobStage.h"
#include "SequenceID.h"
#include "TCAPAnalyzer.h"
#include "ShuffleInfo.h"
#include "DistributedStorageManagerClient.h"
#include <vector>
#include <ExecuteComputation.h>

namespace pdb {

/**
 *  This class is working on Master node to schedule JobStages dynamically from TCAP logical plan
 *  So far following JobStages are supported:
 *
 * -- TupleSetJobStage
 * -- AggregationJobStage
 * -- BroadcastJoinBuildHTJobStage
 * -- HashPartitionJoinBuildHTJobStage
 *
 * Once the QuerySchedulerServer receives a request (A TCAP program) in the form of an @see pdb::ExecuteComputation
 * object it parses and analyzes the TCAP program as a DAG based on a cost model using a greedy algorithm.
 * The goal is to minimize intermediate data.
 * The scheduling is dynamic and lazy, and only one the JobStages scheduled last time
 * were executed, it will schedule later stages, to maximize the information needed.
 * The JobStages will be dispatched to all workers for execution.
 */
class QuerySchedulerServer : public ServerFunctionality {

public:

    /**
     * Constructor for the case when we assume that the port where we are running the server is 8108
     * @param logger an instance of the PDBLogger
     * @param conf
     * @param pseudoClusterMode indicator whether we are running in the pseudo cluster mode or not
     * @param partitionToCoreRatio the ratio between the number of partitions on a node and the number of cores
     */
    QuerySchedulerServer(PDBLoggerPtr logger,
                         ConfigurationPtr conf,
                         bool pseudoClusterMode = false,
                         double partitionToCoreRatio = 0.75);

    /**
     * Constructor for the case when where we specify the port number of the server
     * @param port is the port on which the server that contains this functionality is running
     * @param logger an instance of the PDBLogger
     * @param conf
     * @param pseudoClusterMode indicator whether we are running in the pseudo cluster mode or not
     * @param partitionToCoreRatio the ratio between the number of partitions on a node and the number of cores
     */
    QuerySchedulerServer(int port,
                         PDBLoggerPtr logger,
                         ConfigurationPtr conf,
                         bool pseudoClusterMode = false,
                         double partitionToCoreRatio = 0.75);

    /**
     * Destructor used to destroy the QuerySchedulerServer
     */
    ~QuerySchedulerServer();

    /**
     * This method is from the serverFunctionality interface...
     * It register the query scheduler handlers to the provided server
     */
    void registerHandlers(PDBServer& forMe) override;

    /**
     * Collects the statistics about the sets from
     * each node and updates them [statsForOptimization]
     */
    void collectStats();

    /**
     * Cleans up the query scheduler server so it can be used for the next computation
     */
    void cleanup() override;

    /**
     * Returns the statistics that are being used for optimization
     * @return the statistics
     */
    StatisticsPtr getStats();

    /**
     * Returns the id of the job we are about to run
     * Has the following format Job_Year_Month_Day_Hour-Minute_Second_{Next number from the seqId sequence generator}
     * @return the id of the next job, a string
     */
    std::string getNextJobId() {
        time_t currentTime = time(nullptr);
        struct tm* local = localtime(&currentTime);
        this->jobId = "Job-" + std::to_string(local->tm_year + 1900) + "_" +
            std::to_string(local->tm_mon + 1) + "_" + std::to_string(local->tm_mday) + "_" +
            std::to_string(local->tm_hour) + "_" + std::to_string(local->tm_min) + "_" +
            std::to_string(local->tm_sec) + "_" + std::to_string(seqId.getNextSequenceID());
        return this->jobId;
    }

protected:

    /**
     * Initializes the QuerySchedulerServer more specifically the @see pdb::QuerySchedulerServer#standardResources by
     * fetching the necessary information from the resource manager
     */
    void initialize();

    /**
     * TODO Ask Jia what the difference is between a resource and a node add a proper description
     */
    void initializeForServerMode();

    /**
     * TODO Ask Jia what the difference is between a resource and a node add a proper description
     */
    void initializeForPseudoClusterMode();

    /**
     * This method is used to schedule dynamic pipeline stages
     * It must be invoked after initialize() and before cleanup()
     * @param stagesToSchedule is a vector of all the stages we want to schedule
     * @param shuffleInfo is the shuffle information for job stages that needs repartitioning
     */
    void scheduleStages(std::vector<Handle<AbstractJobStage>>& stagesToSchedule,
                        std::shared_ptr<ShuffleInfo> shuffleInfo);


    /**
     * This method takes in an @see pdb::AbstractJobStage infers its subtype, opens up a communicator to the specified
     * node and schedules the stage at it.
     * @param stage the stage we want to send
     * @param node the node we want to send the stage to
     * @param counter a reference to the counter that needs to be increased once the execution of the stage is complete
     * @param callerBuzzer the buzzer we use to notify the calling thread that we are done executing
     */
    void prepareAndScheduleStage(Handle<AbstractJobStage> &stage,
                                 unsigned long node,
                                 int &counter,
                                 PDBBuzzerPtr &callerBuzzer);

    /**
     * This method schedules a pipeline stage given the index of a specified node and a communicator to that node.
     * @param node is the index of the node we want to schedule the stage
     * @param stage is the pipeline stage we want to schedule
     * @param communicator is the communicator to a node we are going to use for that
     */
    template<typename T>
    bool scheduleStage(unsigned long node,
                       Handle<T>& stage,
                       PDBCommunicatorPtr communicator);

    /**
     * Connects to the node with the provided ip and port
     * and returns an instance of the communicator @see pdb::PDBCommunicator
     * @param port the port of the node
     * @param ip the ip address of the node
     * @return returns null_ptr if it fails, the communicator to the node if it succeeds
     */
    PDBCommunicatorPtr getCommunicatorToNode(int port,
                                             std::string &ip);

    /**
     * Makes a deep copy of the TupleSetJobStage, fills in additional information
     * about the node we are sending it and returns it
     * @param stage an instance of the TupleSetJobStage
     * @return the copy with additional information
     */
    Handle <TupleSetJobStage> getStageToSend(unsigned long index,
                                             Handle <TupleSetJobStage> &stage);

    /**
     * Makes a deep copy of the AggregationJobStage, fills in additional information
     * about the node we are sending it and returns it
     * @param stage an instance of the AggregationJobStage
     * @return the copy with additional information
     */
    Handle <AggregationJobStage> getStageToSend(unsigned long index,
                                                Handle <AggregationJobStage> &stage);

    /**
     * Makes a deep copy of the stage provided and detaches it from the logical plan,
     * by setting it to null and fill in additional information about the node we are sending it to
     * @param stage an instance of the BroadcastJoinBuildHTJobStage
     * @return the copy with additional information
     */
    Handle <BroadcastJoinBuildHTJobStage> getStageToSend(unsigned long index,
                                                         Handle <BroadcastJoinBuildHTJobStage> &stage);

    /**
     * Makes a deep copy of the stage provided and detaches it from the logical plan,
     * by setting it to null and fill in additional information about the node we are sending it to
     *
     * @param stage an instance of the HashPartitionedJoinBuildHTJobStage
     * @return the copy with additional information
     */
    Handle <HashPartitionedJoinBuildHTJobStage> getStageToSend(unsigned long index,
                                                               Handle <HashPartitionedJoinBuildHTJobStage> &stage);

    /**
     * Collects the stats for one node
     * @param node the node we are collecting the stats for
     * @param counter the counter that is increased when we are finishing updating the stats
     * @param callerBuzzer the buzzer that signals that we are finished
     */
    void collectStatsForNode(int node,
                             int &counter,
                             PDBBuzzerPtr &callerBuzzer);

    /**
     * Updates the optimization stats for a given set
     * @param setToUpdateStats the set we are updating (contains also info about the pages and size)
     */
    void updateStats(Handle<SetIdentifier> setToUpdateStats);

    /**
     * This method executes a PDB computation given by the ExecuteComputation object, that was sent by a client
     * @param request the object that describes the computation
     * @param sendUsingMe an instance of the PDBCommunicator that points to the client
     */
    pair<bool, basic_string<char>> executeComputation(Handle<ExecuteComputation> &request,
                                                      PDBCommunicatorPtr &sendUsingMe);


    /**
     * This method finds the best source operator using a heuristic, then uses this operator to extract a sequence of
     * of pipelinable stages.
     * @param jobStageId the of last executed job stage
     * @param jobStages a vector where we want to store the sequence of jobStages
     * @param intermediateSets a vector where we want to store the information about the intermediate sets
     *        that need to be generated
     */
    void extractPipelineStages(int &jobStageId,
                               vector<Handle<AbstractJobStage>> &jobStages,
                               vector<Handle<SetIdentifier>> &intermediateSets);

    /**
     * Given a vector of SetIdentifiers this method issues their creation
     * @param dsmClient an instance of the DistributedStorageManagerClient that needs to create the sets
     * @param intermediateSets the vector of intermediate sets
     */
    void createIntermediateSets(DistributedStorageManagerClient &dsmClient,
                                vector<Handle<SetIdentifier>> &intermediateSets);

    /**
     * This method removes all the intermediate sets that we needed to continue our execution
     * They are kept in the interGlobalSets vector
     * @param dsmClient an instance of the DistributedStorageManagerClient that needs to remove the sets
     */
    void removeIntermediateSets(DistributedStorageManagerClient &dsmClient);

    /**
     * Given a vector of SetIdentifiers this method issues their removal
     * Sets that are going to be used later in the execution are not removed,
     * they are cleaned up with the method @see QuerySchedulerServer#removeIntermediateSets
     * @param dsmClient dsmClient an instance of the DistributedStorageManagerClient that needs to remove the sets
     * @param intermediateSets the vector of intermediate sets
     */
    void removeUnusedIntermediateSets(DistributedStorageManagerClient &dsmClient,
                                      vector<Handle<SetIdentifier>> &intermediateSets);

    /**
     * This method takes in a communicator to a node and issues a request for statistics about the stored sets
     * @param communicator the communicator to the node
     * @param success a reference a boolean that will set to true if the request succeeds, false otherwise
     * @param errMsg the error message that is gonna be set, if an error occurs
     */
    void requestStatistics(PDBCommunicatorPtr &communicator, bool &success, string &errMsg) const;


    /**
     * A vector containing the information about the resources of each node
     */
    std::vector<StandardResourceInfoPtr>* standardResources;

    /**
     * The port through which we access the functionalities on this node (port the PDBServer listens to)
     */
    int port;

    /**
     * Set identifiers for shuffle set, we need to create and remove them at scheduler, so that they
     * exist at any node when any other node needs to write to it
     */
    std::vector<Handle<SetIdentifier>> interGlobalSets;

    /**
     * An instance of the PDBLogger set in the constructor
     */
    PDBLoggerPtr logger;

    /**
     * The configuration of the node provided by the constructor
     */
    ConfigurationPtr conf;

    /**
     * True if we are running PDB in pseudo cluster mode false otherwise
     */
    bool pseudoClusterMode;

    /**
     * This is used to synchronize the communicator
     * More specifically the part where we are creating them and connecting them to a remote node
     * with the connectToInternetServer method
     */
    pthread_mutex_t connection_mutex;

    /**
     * Used to generate a unique sequential ID
     * getNextSequenceID is thread safe to call
     */
    SequenceID seqId;

    /**
     * The id of the current job. Used to identify the job and the database for it's results
     */
    std::string jobId;

    /**
     * Used to calculate the the number of partitions on a node, based on the number cpu cores on that node
     * more specifically partitionToCoreRatio = numPartitionsOnThisNode/numCores
     */
    double partitionToCoreRatio;

    /**
     * An instance of the TCAPAnalyzer. We use it to do the dynamic planning.
     * More specifically to find the source that is we should start from using heuristics and
     * to generate pipeline stages starting from this source
     */
    std::shared_ptr<TCAPAnalyzer> tcapAnalyzerPtr;

    /**
     * Contains the information about every set on every node,
     * more specifically :
     * 1. the number of pages
     * 2. the size of the pages
     * 3. and the total number of bytes the set has
     */
    StatisticsPtr statsForOptimization;

    /**
     * Wraps shuffle information for job stages that needs repartitioning data
     */
    std::shared_ptr<ShuffleInfo> shuffleInfo;
};
}


#endif
