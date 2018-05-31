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

#ifndef REGISTER_REPLICA_H
#define REGISTER_REPLICA_H

#include "Object.h"
#include "PDBString.h"
#include "Computation.h"

// PRELOAD %RegisterReplica%

namespace pdb {

// encapsulates a request to run a register a replica with the statistics database
class RegisterReplica : public Object {

public:
    //default constructor
    RegisterReplica() {}

    //destructor
    ~RegisterReplica() {}

    /*
     * constructor
     * @param inputDatabaseAndSet: input set that is used to generate the replica
     * @param outputDatabaseAndSet: output set to store the replica
     * @param numPartitions: number of partitions
     * @param numNodes: number of nodes
     * @param replicaType: type of operations that transforms the input set to the output set
     * @param tcapString: tcapString related with the output
     */
    RegisterReplica(std::pair<std::string, std::string> inputDatabaseAndSet,
                    std::pair<std::string, std::string> outputDatabaseAndSet,
                    int numPartitions,
                    int numNodes,
                    std::string replicaType,
                    std::string tcapString,
                    std::vector<Handle<Computation>> inputComputations) {
        this->inputDatabaseName = inputDatabaseAndSet.first;
        this->inputSetName = inputDatabaseAndSet.second;
        this->outputDatabaseName = outputDatabaseAndSet.first;
        this->outputSetName = outputDatabaseAndSet.second;
        this->numPartitions = numPartitions;
        this->numNodes = numNodes;
        this->replicaType = replicaType;
        this->tcapString = tcapString;
        this->computations = makeObject<Vector<Handle<Computation>>>();
        size_t numComputations = inputComputations.size();
        for (size_t i=0; i < numComputations; i++) {
            this->computations->push_back(inputComputations[i]);
        }
    }

    /*
     * getters/setters
     */

    void setTCAPString(std::string tcapString) {
        this->tcapString = tcapString;
    }

    std::string getTCAPString() {
        return tcapString;
    }

    void setInputDatabaseName(std::string inputDatabaseName) {
        this->inputDatabaseName = inputDatabaseName;
    }

    std::string getInputDatabaseName() {
        return this->inputDatabaseName;
    }

    void setInputSetName(std::string inputSetName) {
        this->inputSetName = inputSetName;
    }

    std::string getInputSetName() {
        return this->inputSetName;
    }

    void setOutputDatabaseName(std::string outputDatabaseName) {
        this->outputDatabaseName = outputDatabaseName;
    }

    std::string getOutputDatabaseName() {
        return this->outputDatabaseName;
    }

    void setOutputSetName(std::string outputSetName) {
        this->outputSetName = outputSetName;
    }
    
    std::string getOutputSetName() {
        return this->outputSetName;
    }

    void setNumPartitions(int numPartitions) {
        this->numPartitions = numPartitions;
    }

    int getNumPartitions() {
        return this->numPartitions;
    }

    void setNumNodes(int numNodes) {
        this->numNodes = numNodes;
    }

    int getNumNodes() {
        return this->numNodes;
    }


    void setReplicaType (std::string replicaType) {
        this->replicaType = replicaType;
    }

    std::string getReplicaType() {
        return replicaType;
    }

    Handle<Vector<Handle<Computation>>> getComputations() {
        return computations;
    }

    void print() {
        std::cout << "inputDatabaseName is " << inputDatabaseName << std::endl;
        std::cout << "inputSetName is " << inputSetName << std::endl;
        std::cout << "outputDatabaseName is " << outputDatabaseName << std::endl;
        std::cout << "outputSetName is " << outputSetName << std::endl;
        std::cout << "number of computation: " << computations->size() << std::endl;
    }

    ENABLE_DEEP_COPY

private:
    //input database name
    String inputDatabaseName;

    //input set name
    String inputSetName;

    //output database name
    String outputDatabaseName;

    //output set name
    String outputSetName;

    //number of partitions
    int numPartitions;

    //number of nodes
    int numNodes;

    //type of replica
    String replicaType;

    //tcap string that is used to produce replica
    String tcapString;

    //computations
    Handle<Vector<Handle<Computation>>> computations = nullptr;   

};
}

#endif
