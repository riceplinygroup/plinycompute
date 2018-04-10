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


#include "PDBDebug.h"
#include "Object.h"
#include "DataTypes.h"
#include "Handle.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "SetIdentifier.h"
#include "ComputePlan.h"
#include "AbstractJobStage.h"
#include <stdlib.h>

// PRELOAD %TupleSetJobStage%

namespace pdb {

class TupleSetJobStage : public AbstractJobStage {

public:
    // constructor
    TupleSetJobStage() {}

    // constructor
    TupleSetJobStage(JobStageID stageId) {
        this->id = stageId;
        this->sharedPlan = nullptr;
        this->sourceContext = nullptr;
        this->hashContext = nullptr;
        this->sinkContext = nullptr;
        this->probeOrNot = false;
        this->repartitionOrNot = false;
        this->combineOrNot = false;
        this->broadcastOrNot = false;
        this->inputAggHashOutOrNot = false;
        this->numNodes = 0;
        this->numPartitions = nullptr;
        this->ipAddresses = nullptr;
    }

    // constructor
    TupleSetJobStage(JobStageID stageId, int numNodes) {
        this->id = stageId;
        this->sharedPlan = nullptr;
        this->sourceContext = nullptr;
        this->hashContext = nullptr;
        this->sinkContext = nullptr;
        this->probeOrNot = false;
        this->repartitionOrNot = false;
        this->broadcastOrNot = false;
        this->combineOrNot = false;
        this->inputAggHashOutOrNot = false;
        this->numNodes = numNodes;
        this->numPartitions = makeObject<Vector<Handle<Vector<HashPartitionID>>>>(numNodes);
        this->ipAddresses = makeObject<Vector<String>>(numNodes);
    }

    ~TupleSetJobStage() {}

    std::string getJobStageType() override {
        return "TupleSetJobStage";
    }

    int16_t getJobStageTypeID() override {
        return TupleSetJobStage_TYPEID;
    }

    // to set compute plan
    void setComputePlan(const Handle<ComputePlan> &plan,
                        const std::string &sourceTupleSetSpecifier,
                        const std::string &targetTupleSetSpecifier,
                        const std::string &targetComputationSpecifier) {
        this->sharedPlan = plan;
        this->sourceTupleSetSpecifier = sourceTupleSetSpecifier;
        this->targetTupleSetSpecifier = targetTupleSetSpecifier;
        this->targetComputationSpecifier = targetComputationSpecifier;
    }
    // to get source tupleset name for this pipeline stage
    std::string getSourceTupleSetSpecifier() {
        return this->sourceTupleSetSpecifier;
    }
    // to get target tupleset name for this pipeline stage
    std::string getTargetTupleSetSpecifier() {
        return this->targetTupleSetSpecifier;
    }
    // to get target computation name for this pipeline stage
    std::string getTargetComputationSpecifier() {
        return this->targetComputationSpecifier;
    }

    // to get the vector of tuple sets to build the pipeline
    void getTupleSetsToBuildPipeline(std::vector<std::string>& buildMe) {
        buildMe.clear();
        for (int i = 0; i < (*buildTheseTupleSets).size(); i++) {
            buildMe.push_back((*buildTheseTupleSets)[i]);
        }
    }

    // to set tuplesets for building pipeline
    void setTupleSetsToBuildPipeline(const std::vector<std::string> &buildMe) {
        buildTheseTupleSets = makeObject<Vector<String>>();
        for (const auto &i : buildMe) {
            buildTheseTupleSets->push_back(i);
        }
    }

    // nullify compute plan shared pointer
    void nullifyComputePlanPointer() {
        this->sharedPlan->nullifyPlanPointer();
    }


    // to get compute plan
    Handle<ComputePlan> getComputePlan() {
        return this->sharedPlan;
    }

    // to set source set identifier
    void setSourceContext(Handle<SetIdentifier> sourceContext) {
        this->sourceContext = sourceContext;
    }

    // to return source set identifier
    Handle<SetIdentifier> getSourceContext() {
        return this->sourceContext;
    }

    // to set sink set identifier
    void setSinkContext(Handle<SetIdentifier> sinkContext) {
        this->sinkContext = sinkContext;
    }

    // to return sink set identifier
    Handle<SetIdentifier> getSinkContext() {
        return this->sinkContext;
    }

    // to set hash set identifier for probing
    void setHashContext(Handle<SetIdentifier> hashContext) {
        this->hashContext = hashContext;
    }

    // to return hash set identifier for probing
    Handle<SetIdentifier> getHashContext() {
        return this->hashContext;
    }

    // to set combiner set identifier for combining data
    void setCombinerContext(Handle<SetIdentifier> combinerContext) {
        this->combinerContext = combinerContext;
    }

    // to return combiner set identifier for combining data
    Handle<SetIdentifier> getCombinerContext() {
        return this->combinerContext;
    }

    // to set whether to probe hash table
    void setProbing(bool probeOrNot) {
        this->probeOrNot = probeOrNot;
    }

    // to return whether this stage requires to probe hash table
    bool isProbing() {
        return this->probeOrNot;
    }


    // to set whether to probe hash table
    void setInputAggHashOut(bool inputAggHashOutOrNot) {
        this->inputAggHashOutOrNot = inputAggHashOutOrNot;
    }

    // to return whether this stage requires to probe hash table
    bool isInputAggHashOut() {
        return this->inputAggHashOutOrNot;
    }


    // to set whether to broadcast hash table
    void setBroadcasting(bool broadcastOrNot) {
        this->broadcastOrNot = broadcastOrNot;
    }

    // to return whether to broadcast hash table
    bool isBroadcasting() {
        return this->broadcastOrNot;
    }

    // to set whether to repartition the output
    void setRepartition(bool repartitionOrNot) {
        this->repartitionOrNot = repartitionOrNot;
    }

    // to return whether this stage requires to repartition output
    bool isRepartition() {
        return this->repartitionOrNot;
    }

    // to set whether to repartition the output for a join
    void setRepartitionJoin(bool repartitionJoinOrNot) {
        this->repartitionJoinOrNot = repartitionJoinOrNot;
    }

    // to return whether this stage requires to repartition output for a join
    bool isRepartitionJoin() {
        return this->repartitionJoinOrNot;
    }

    // to set whether to repartition the output into vectors
    void setRepartitionVector(bool repartitionVectorOrNot) {
        this->repartitionVectorOrNot = repartitionVectorOrNot;
    }

    // to return whether this stage requires to repartition output into vectors
    bool isRepartitionVector() {
        return this->repartitionVectorOrNot;
    }


    // to set whether to combine the repartitioned output
    void setCombining(bool combineOrNot) {
        this->combineOrNot = combineOrNot;
    }

    // to return whether this stage requires to combine repartitioned output
    bool isCombining() {
        return this->combineOrNot;
    }

    JobStageID getStageId() override {
        return this->id;
    }

    void setCollectAsMap(bool collectAsMapOrNot) {
        this->collectAsMapOrNot = collectAsMapOrNot;
    }

    bool isCollectAsMap() {
        return this->collectAsMapOrNot;
    }

    int getNumNodesToCollect() {
        return this->numNodesToCollect;
    }

    void setNumNodesToCollect(int numNodesToCollect) {
        this->numNodesToCollect = numNodesToCollect;
    }


    void print() override {
        std::cout << "[JOB ID] jobId=" << jobId << std::endl;
        std::cout << "[STAGE ID] id=" << id << std::endl;
        std::cout << "[INPUT] databaseName=" << sourceContext->getDatabase()
                  << ", setName=" << sourceContext->getSetName() << std::endl;
        std::cout << "[OUTPUT] databaseName=" << sinkContext->getDatabase()
                  << ", setName=" << sinkContext->getSetName() << std::endl;
        std::cout << "[OUTTYPE] typeName=" << getOutputTypeName() << std::endl;
        std::cout << "[SOURCE TUPLESET] sourceTupleSetSpecifier=" << this->sourceTupleSetSpecifier
                  << std::endl;
        std::cout << "[TARGET TUPLESET] targetTupleSetSpecifier=" << this->targetTupleSetSpecifier
                  << std::endl;
        std::cout << "[TARGET COMPUTATION] targetComputationSpecifier="
                  << this->targetComputationSpecifier << std::endl;
        std::cout << "[ALLOCATOR POLICY] allocatorPolicy=" << this->allocatorPolicy << std::endl;
        if (buildTheseTupleSets != nullptr) {
            std::cout << "[PIPELINE]" << std::endl;
            size_t mySize = buildTheseTupleSets->size();
            for (size_t i = 0; i < mySize; i++) {
                std::cout << i << ": " << (*buildTheseTupleSets)[i] << std::endl;
            }
        }
        std::cout << "[Probing] isProbing=" << this->probeOrNot << std::endl;
        std::cout << "Number of cluster nodes=" << getNumNodes() << std::endl;
        std::cout << "Total memory on this node is " << totalMemoryOnThisNode << std::endl;
        std::cout << "Number of total partitions=" << getNumTotalPartitions() << std::endl;
        int i;
        for (i = 0; i < numNodes; i++) {
            Handle<Vector<HashPartitionID>> partitions = getNumPartitions(i);
            std::cout << "Number of partitions on node-" << i << " is " << partitions->size()
                      << std::endl;
            std::cout << "IP address on node-" << i << " is " << getIPAddress(i) << std::endl;
            int port;
            if ((port = getPort(i)) > 0) {
                std::cout << "Port on node-" << i << " is " << port << std::endl;
            }
        }
    }

    std::string getOutputTypeName() {
        return this->outputTypeName;
    }

    void setOutputTypeName(const std::string &outputTypeName) {
        this->outputTypeName = outputTypeName;
    }

    void setNumNodes(int numNodes) {
        this->numNodes = numNodes;
    }

    int getNumNodes() {
        return this->numNodes;
    }

    void addNumPartitions(Handle<Vector<HashPartitionID>> numPartitions) {
        this->numPartitions->push_back(numPartitions);
    }

    Handle<Vector<HashPartitionID>>& getNumPartitions(int nodeId) {
        return (*numPartitions)[nodeId];
    }

    Handle<Map<String, String>>& getHashSets() {
        return this->hashSetsToProbe;
    }

    void addHashSetToProbe(String targetTupleSetName, String hashSetName) {
        if (hashSetsToProbe == nullptr) {
            hashSetsToProbe = makeObject<Map<String, String>>();
        }
        (*hashSetsToProbe)[targetTupleSetName] = hashSetName;
    }

    void setHashSetsToProbe(Handle<Map<String, String>> hashSetsToProbe) {
        this->hashSetsToProbe = hashSetsToProbe;
    }

    String getIPAddress(int nodeId) {
        if ((unsigned int)nodeId < numPartitions->size()) {
            std::string ipStr = (*ipAddresses)[nodeId];
            if (ipStr.find(':') != std::string::npos) {
                std::string ip = ipStr.substr(0, ipStr.find(':'));
                return String(ip);
            } else {
                return String(ipStr);
            }
        } else {
            return nullptr;
        }
    }

    int getPort(int nodeId) {
        if ((unsigned int)nodeId < numPartitions->size()) {
            std::string ipStr = (*ipAddresses)[nodeId];
            if (ipStr.find(':') != std::string::npos) {
                int port = atoi((ipStr.substr(ipStr.find(':') + 1)).c_str());
                return port;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    // each address could be simply an IP address like 10.134.96.50, or IP address and port pair
    // like localhost:8109
    void setIPAddresses(Handle<Vector<String>> addresses) {
        this->ipAddresses = addresses;
    }

    void setNumPartitions(Handle<Vector<Handle<Vector<HashPartitionID>>>> numPartitions) {
        this->numPartitions = numPartitions;
    }


    int getNumTotalPartitions() {
        return this->numTotalPartitions;
    }

    void setNumTotalPartitions(int numTotalPartitions) {
        this->numTotalPartitions = numTotalPartitions;
    }

    NodeID getNodeId() {
        return this->myNodeId;
    }

    void setNodeId(NodeID nodeId) {
        this->myNodeId = nodeId;
    }

    void setTotalMemoryOnThisNode(size_t totalMem) {
        this->totalMemoryOnThisNode = totalMem;
    }

    size_t getTotalMemoryOnThisNode() {
        return this->totalMemoryOnThisNode;
    }

    void setAllocatorPolicy(AllocatorPolicy myPolicy) {
        this->allocatorPolicy = myPolicy;
    }

    AllocatorPolicy getAllocatorPolicy() {
        return this->allocatorPolicy;
    }

    ENABLE_DEEP_COPY


private:
    // Input set information
    Handle<SetIdentifier> sourceContext = nullptr;

    // Hash set information for probing aggregation set
    Handle<SetIdentifier> hashContext = nullptr;

    // Combiner set information
    Handle<SetIdentifier> combinerContext = nullptr;

    // Output set information
    Handle<SetIdentifier> sinkContext = nullptr;

    // Output type name
    String outputTypeName;

    // logical plan information
    Handle<ComputePlan> sharedPlan = nullptr;

    // source tuple set
    String sourceTupleSetSpecifier;

    // target tuple set
    String targetTupleSetSpecifier;

    // target computation
    String targetComputationSpecifier;

    // tuple sets to build the pipeline
    Handle<Vector<String>> buildTheseTupleSets = nullptr;

    // Does this stage has a PartitionedJoinSink that partitions JoinMaps
    bool repartitionJoinOrNot = false;

    // Does this stage has a HashPartitionSink that partitions Vectors
    bool repartitionVectorOrNot = false;

    // Does this stage require probing a hash table ?
    bool probeOrNot = false;

    // Does this stage require repartitioning output?
    bool repartitionOrNot = false;

    // Does this stage require combining repartitioned results?
    bool combineOrNot = false;

    // Does this stage require broadcasting results?
    bool broadcastOrNot = false;

    // Does this stage consume aggregation hash output?
    bool inputAggHashOutOrNot = false;

    // Does this stage collect results to one partition?
    bool collectAsMapOrNot = false;

    // Number of nodes to collect aggregation results
    int numNodesToCollect = 1;

    // hash set names to probe for join
    Handle<Map<String, String>> hashSetsToProbe = nullptr;

    // the id to identify this job stage
    JobStageID id;

    // repartitioning scheme
    int numNodes;

    // number of partitions on each node
    Handle<Vector<Handle<Vector<HashPartitionID>>>> numPartitions = nullptr;

    // IP for each node
    Handle<Vector<String>> ipAddresses;

    // totalPartitions, should be consistent with numPartitions
    int numTotalPartitions;

    // node id of the receiver of this message
    NodeID myNodeId;

    // memory size on this node
    size_t totalMemoryOnThisNode;

    // allocator policy
    AllocatorPolicy allocatorPolicy;
};
}

#endif
