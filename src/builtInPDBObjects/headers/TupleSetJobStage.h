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
#ifndef TUPLESET_JOBSTAGE_H
#define TUPLESET_JOBSTAGE_H

//by Jia, Mar 5th, 2017

#include "PDBDebug.h"
#include "Object.h"
#include "DataTypes.h"
#include "Handle.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "SetIdentifier.h"
#include "ComputePlan.h"
#include "AbstractJobStage.h"

// PRELOAD %TupleSetJobStage%

namespace pdb {

    class TupleSetJobStage : public AbstractJobStage {
        
    public:

            TupleSetJobStage () {}
             
            TupleSetJobStage (JobStageID stageId) {
                this->id = stageId;
                this->sharedPlan = nullptr;
                this->sourceContext = nullptr;
                this->hashContext = nullptr;
                this->sinkContext = nullptr;
                this->probeOrNot = false;
                this->repartitionOrNot = false;
                this->combineOrNot = false;
                this->numNodes = 0;
                this->numPartitions = nullptr;
                this->ipAddresses = nullptr;
            }

            TupleSetJobStage (JobStageID stageId, int numNodes) {
                this->id = stageId;
                this->sharedPlan = nullptr;
                this->sourceContext = nullptr;
                this->hashContext = nullptr;
                this->sinkContext = nullptr;
                this->probeOrNot = false;
                this->repartitionOrNot = false;
                this->combineOrNot = false;
                this->numNodes = numNodes;
                this->numPartitions = makeObject<Vector<Handle<Vector<HashPartitionID>>>> (numNodes);
                this->ipAddresses = makeObject<Vector<String>> (numNodes);
            }

            ~TupleSetJobStage () {}


            std :: string getJobStageType() override { return "TupleSetJobStage"; }
 
            //to set compute plan
            void setComputePlan( Handle<ComputePlan> plan, std::string sourceTupleSetSpecifier, std::string targetTupleSetSpecifier, std::string targetComputationSpecifier ) {
                this->sharedPlan = plan;
                this->sourceTupleSetSpecifier = sourceTupleSetSpecifier;
                this->targetTupleSetSpecifier = targetTupleSetSpecifier;
                this->targetComputationSpecifier = targetComputationSpecifier;
            } 

            std::string getSourceTupleSetSpecifier() {
                return this->sourceTupleSetSpecifier;
            }

            std::string getTargetTupleSetSpecifier() {
                return this->targetTupleSetSpecifier;
            }

            std::string getTargetComputationSpecifier() {
                return this->targetComputationSpecifier;
            }

            //to get compute plan
            Handle<ComputePlan> getComputePlan() {
                return this->sharedPlan;
            }

            //to set source set identifier
            void setSourceContext( Handle<SetIdentifier> sourceContext ) {
                this->sourceContext = sourceContext;
            }

            //to return source set identifier
            Handle<SetIdentifier> getSourceContext() {
                return this->sourceContext;
            }

            //to set sink set identifier
            void setSinkContext( Handle<SetIdentifier> sinkContext ) {
                this->sinkContext = sinkContext;
            }
            
            //to return sink set identifier
            Handle<SetIdentifier> getSinkContext() {
                return this->sinkContext;
            } 

            //to set hash set identifier for probing
            void setHashContext( Handle<SetIdentifier> hashContext ) {
                this->hashContext = hashContext;
            }

            //to return hash set identifier for probing
            Handle<SetIdentifier> getHashContext() {
                return this->hashContext;
            }

            //to set combiner set identifier for combining data
            void setCombinerContext( Handle<SetIdentifier> combinerContext ) {
                this->combinerContext = combinerContext;
            }

            //to return combiner set identifier for combining data
            Handle<SetIdentifier> getCombinerContext() {
                return this->combinerContext;
            }

            //to set whether to probe hash table
            void setProbing (bool probeOrNot) {
                this->probeOrNot = probeOrNot;
            }            

            //to return whether this stage requires to probe hash table
            bool isProbing() {
                return this->probeOrNot;
            }

            //to set whether to repartition the output
            void setRepartition (bool repartitionOrNot) {
                this->repartitionOrNot = repartitionOrNot;
            }

            //to return whether this stage requires to repartition output
            bool isRepartition() {
                return this->repartitionOrNot;
            }

            //to set whether to combine the repartitioned output
            void setCombining (bool combineOrNot) {
                this->combineOrNot = combineOrNot;
            }

            //to return whether this stage requires to combine repartitioned output
            bool isCombining() {
                return this->combineOrNot;
            }

            JobStageID getStageId() {
                return this->id;
            }

            void print() override {
                std :: cout << "[JOB ID] jobId=" << jobId << std :: endl;
                std :: cout << "[STAGE ID] id=" << id << std :: endl;
                std :: cout << "[INPUT] databaseName=" << sourceContext->getDatabase()<<", setName=" << sourceContext->getSetName()<< std :: endl;
                std :: cout << "[OUTPUT] databaseName=" << sinkContext->getDatabase()<<", setName=" << sinkContext->getSetName()<< std :: endl;
                std :: cout << "[OUTTYPE] typeName=" << getOutputTypeName() << std :: endl;
                std :: cout << "[SOURCE TUPLESET] sourceTupleSetSpecifier=" << this->sourceTupleSetSpecifier << std :: endl;
                std :: cout << "[TARGET TUPLESET] targetTupleSetSpecifier=" << this->targetTupleSetSpecifier << std :: endl;
                std :: cout << "[TARGET COMPUTATION] targetComputationSpecifier=" << this->targetComputationSpecifier << std :: endl;
                std :: cout << "Number of cluster nodes=" << getNumNodes() << std :: endl;
                std :: cout << "Number of total partitions=" << getNumTotalPartitions() << std :: endl;
                int i; 
                for ( i = 0; i < numNodes; i ++) {
                    Handle<Vector<HashPartitionID>> partitions = getNumPartitions(i);
                    std :: cout << "Number of partitions on node-" << i << " is " << partitions->size()<< std :: endl;
                    std :: cout << "IP address on node-" << i << " is " << getIPAddress(i) << std :: endl;
                }
            }

            std :: string getOutputTypeName () {
                return this->outputTypeName;
            }

            void setOutputTypeName (std :: string outputTypeName) {
                this->outputTypeName = outputTypeName;
            }

            void setNumNodes (int numNodes) {
                this->numNodes = numNodes;
            }

            int getNumNodes () {
                return this->numNodes;
            }

            void addNumPartitions(Handle<Vector<HashPartitionID>> numPartitions) {
                this->numPartitions->push_back(numPartitions);
            }

            Handle<Vector<HashPartitionID>> & getNumPartitions (int nodeId) {
                    return (*numPartitions)[nodeId];
            } 

            String getIPAddress (int nodeId) {
                if ((unsigned int) nodeId < numPartitions->size()) {
                    return (*ipAddresses)[nodeId];
                } else {
                    return nullptr;
                }
            }

            void setIPAddresses (Handle<Vector<String>> addresses) {
                this->ipAddresses = addresses;
            }

            void setNumPartitions (Handle<Vector<Handle<Vector<HashPartitionID>>>> numPartitions) {
                this->numPartitions = numPartitions;
            }


            int getNumTotalPartitions () {
                return this->numTotalPartitions;
            }

            void setNumTotalPartitions (int numTotalPartitions) {
                this->numTotalPartitions = numTotalPartitions;
            }

            NodeID getNodeId () {
                return this->myNodeId;
            }

            void setNodeId (NodeID nodeId) {
                this->myNodeId = nodeId;
            }

            bool getNeedsRemoveInputSet () {
                return this->needsRemoveInputSet;
            }

            void setNeedsRemoveInputSet (bool needsRemoveInputSet) {
                this->needsRemoveInputSet = needsRemoveInputSet;
            }

            bool getNeedsRemoveCombinerSet () {
                return this->needsRemoveCombinerSet;
            }

            void setNeedsRemoveCombinerSet (bool needsRemoveCombinerSet) {
                this->needsRemoveCombinerSet = needsRemoveCombinerSet;
            }


            ENABLE_DEEP_COPY


    private:

            //Input set information
            Handle<SetIdentifier> sourceContext;

            //Hash set information for probing
            Handle<SetIdentifier> hashContext;

            //Combiner set information
            Handle<SetIdentifier> combinerContext;

            //Output set information
            Handle<SetIdentifier> sinkContext;

            //Output type name
            String outputTypeName;

            //logical plan information
            Handle<ComputePlan> sharedPlan;

            //source tuple set
            String sourceTupleSetSpecifier;

            //target tuple set
            String targetTupleSetSpecifier;

            //target computation
            String targetComputationSpecifier;


            //Does this stage require probing a hash table ?
            bool probeOrNot;

            //Does this stage require repartitioning output?
            bool repartitionOrNot;

            //Does this stage require combining repartitioned results?
            bool combineOrNot;

            //the id to identify this job stage
            JobStageID id;

            // repartitioning scheme
            int numNodes;

            // number of partitions on each node
            Handle<Vector<Handle<Vector<HashPartitionID>>>> numPartitions;

            // IP for each node
            Handle<Vector<String>> ipAddresses;

            // totalPartitions, should be consistent with numPartitions
            int numTotalPartitions;

            // node id of the receiver of this message
            NodeID myNodeId;

            // needs remove combiner set at frontend
            bool needsRemoveCombinerSet;

            // needs remove input set at frontend
            bool needsRemoveInputSet;


   };

}

#endif
