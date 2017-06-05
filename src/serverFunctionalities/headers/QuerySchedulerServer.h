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

//by Jia, Sept 2016

#include "ServerFunctionality.h"
#include "ResourceInfo.h"
#include "StandardResourceInfo.h"
#include "Handle.h"
#include "PDBVector.h"
#include "QueryBase.h"
#include "ResourceInfo.h"
#include "JobStage.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "PDBLogger.h"
#include "QueryGraphIr.h"
#include "TupleSetJobStage.h"
#include "AggregationJobStage.h"
#include "BroadcastJoinBuildHTJobStage.h"
#include "SequenceID.h"
#include "TCAPAnalyzer.h"
#include "ShuffleInfo.h"
#include <vector>

namespace pdb {

class QuerySchedulerServer : public ServerFunctionality {

public:

       //destructor
       ~QuerySchedulerServer ();

       //constructor for the case when query scheduler is co-located with resource manager       
       QuerySchedulerServer(PDBLoggerPtr logger, bool pseudoClusterMode = false, double partitionToCoreRatio = 0.75, bool isDynamicPlanning = true, bool removeIntermediateDataEarly = false);

       QuerySchedulerServer(int port, PDBLoggerPtr logger, bool pseudoClusterMode = false, double partitionToCoreRatio = 0.75,  bool isDynamicPlanning = true, bool removeIntermediateDataEarly = false);


       //constructor for the case when query scheduler and resource manager are in two different nodes
       QuerySchedulerServer (std :: string resourceManagerIp, int port, PDBLoggerPtr logger, bool usePipelineNetwork = false, double partitionToCoreRatio = 0.75,  bool isDynamicPlanning = true, bool removeIntermediateDataEarly = false);

       //initialization
       void initialize(bool isRMRunAsServer);


       //deprecated
       //to transform optimized client query into a physical plan
       //each pipeline can have more than one output
       void parseOptimizedQuery(pdb_detail::QueryGraphIrPtr queryGraph);

       //to replace above two methods to automatically build the physical plan based on TCAP string and computations
       bool parseTCAPString(Handle<Vector<Handle<Computation>>> myComputations, std :: string myTCAPString);


       //deprecated
       //to print parsed physical execution plan
       void printCurrentPlan();

       //to replace printCurrentPlan()
       void printStages();

       //deprecated
       //to schedule the current job plan
       bool schedule(std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode);

       //to schedule dynamic pipeline stages
       void scheduleStages(std :: vector <Handle<AbstractJobStage>> & stagesToSchedule, std :: vector <Handle<SetIdentifier>> & intermediateSets, std :: shared_ptr<ShuffleInfo> shuffleInfo);


       //deprecated
       //to schedule a job stage
       bool schedule(Handle<JobStage> &stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode);

       //Jia: one TODO is to consolidate below three functions into one function.
       //to replace: bool schedule(Handle<JobStage> &stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode)
       //to schedule a pipeline stage
       bool scheduleStage(int index, Handle<TupleSetJobStage> &stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode);
       bool scheduleStage(int index, Handle<AggregationJobStage> &stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode);
       bool scheduleStage(int index, Handle<BroadcastJoinBuildHTJobStage> &stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode);
       //deprecated
       //to schedule the current job plan on all available resources
       void schedule();

       //to replace: void schedule()
       //to schedule the query plan on all available resources
       void scheduleQuery();


       //from the serverFunctionality interface... register the resource manager handlers
       void registerHandlers (PDBServer &forMe) override;

       void cleanup () override;       

       //collect the statistics that will be used for optimizer
       //this needs the functionality of catalog and distributed storage manager
       void initializeStats ();
      
       //return statsForOptimization
       StatisticsPtr getStats ();

       //deprecated
       Handle<SetIdentifier>  getOutputSet() {
           return currentPlan[0]->getOutput();
       }

       //deprecated
       std :: string getOutputTypeName() {
           return currentPlan[0]->getOutputTypeName();
       }


       std :: string getNextJobId() {
           time_t currentTime = time(NULL);
           struct tm *local = localtime(&currentTime);
           this->jobId = "Job-"+std::to_string(local->tm_year+1900)+"_"+std::to_string(local->tm_mon+1)+"_"+std::to_string(local->tm_mday)+"_"+std::to_string(local->tm_hour)+"_"+std::to_string(local->tm_min)+"_"+std::to_string(local->tm_sec)+"_"+std::to_string(seqId.getNextSequenceID());
           return this->jobId;
       }

       void collectStats ();
       void updateStats (Handle<SetIdentifier> setToUpdateStats);

protected:

       //current resources (deprecated, and we should use standardResources in our code)
       //Handle<Vector<Handle<ResourceInfo>>> resources;

       //current resources
       std :: vector<StandardResourceInfoPtr> * standardResources;

       // resource manager IP address
       std :: string resourceManagerIp;

       // port number
       int port;

       // deprecated
       // physical plan that is temporary, however each query scheduler can schedule one JobStage at each time, similar with Spark/Hadoop
       std :: vector<Handle<JobStage>> currentPlan;

       // use TupleSetJobStage/AggregationJobStage to replace JobStage
       std :: vector<Handle<AbstractJobStage>> queryPlan;

       //set identifiers for shuffle set, we need to create and remove them at scheduler, so that they exist at any node when any other node needs to write to it
       std :: vector<Handle<SetIdentifier>> interGlobalSets;


       // logger
       PDBLoggerPtr logger;

       bool usePipelineNetwork;

       bool pseudoClusterMode; 

       pthread_mutex_t connection_mutex;

       JobStageID jobStageId;

       SequenceID seqId;

       std :: string jobId;

       double partitionToCoreRatio;

       //below variables are added for dynamic planning

       bool dynamicPlanningOrNot;

       bool earlyRemovingDataOrNot;

       std :: shared_ptr<TCAPAnalyzer> tcapAnalyzerPtr;

       StatisticsPtr statsForOptimization;

       std :: shared_ptr<ShuffleInfo> shuffleInfo;
};


}



#endif
