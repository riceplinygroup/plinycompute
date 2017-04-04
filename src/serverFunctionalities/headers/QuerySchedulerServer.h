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
#include "JobStage.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "PDBLogger.h"
#include "QueryGraphIr.h"
#include "TupleSetJobStage.h"
#include "AggregationJobStage.h"
#include "SequenceID.h"
#include <vector>

namespace pdb {

class QuerySchedulerServer : public ServerFunctionality {

public:

       //destructor
       ~QuerySchedulerServer ();

       //constructor for the case when query scheduler is co-located with resource manager       
       QuerySchedulerServer(PDBLoggerPtr logger, bool pseudoClusterMode = false);

       QuerySchedulerServer(int port, PDBLoggerPtr logger, bool pseudoClusterMode = false);


       //constructor for the case when query scheduler and resource manager are in two different nodes
       QuerySchedulerServer (std :: string resourceManagerIp, int port, PDBLoggerPtr logger, bool usePipelineNetwork = false);

       //initialization
       void initialize(bool isRMRunAsServer);

       //deprecated
       //to transform optimized client query into a physical plan
       //each pipeline can have more than one output
       void parseOptimizedQuery(pdb_detail::QueryGraphIrPtr queryGraph);

       //deprecated
       //to replace parseOptimizedQuery to build the physical plan
       void parseQuery(Vector<Handle<Computation>> myComputations, String myTCAPString);

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

       //to replace: bool schedule(std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode)
       //to schedule pipeline stages
       bool scheduleStages(int index, std :: string ip, int port, PDBLoggerPtr logger, ObjectCreationMode mode);

       //deprecated
       //to schedule a job stage
       bool schedule(Handle<JobStage> &stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode);

       //to replace: bool schedule(Handle<JobStage> &stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode)
       //to schedule a pipeline stage
       bool scheduleStage(int index, Handle<TupleSetJobStage> &stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode);

       bool scheduleStage(int index, Handle<AggregationJobStage> &stage, PDBCommunicatorPtr communicator, ObjectCreationMode mode);

       //deprecated
       //to schedule the current job plan on all available resources
       void schedule();

       //to replace: void schedule()
       //to schedule the query plan on all available resources
       void scheduleQuery();


       //deprecated
       //to transform user query to tcap string
       String transformQueryToTCAP(Vector<Handle<Computation>> myComputations, int flag=0);


       //from the serverFunctionality interface... register the resource manager handlers
       void registerHandlers (PDBServer &forMe) override;

       void cleanup () override;       

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

//       Handle<QueriesAndPlan> newQueriesAndPlan;

       bool pseudoClusterMode; 

       pthread_mutex_t connection_mutex;

       JobStageID jobStageId;

       SequenceID seqId;

       std :: string jobId;
};


}



#endif
