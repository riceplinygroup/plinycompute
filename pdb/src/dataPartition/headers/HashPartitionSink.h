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

#ifndef HASH_PARTITION_SINK_H
#define HASH_PARTITION_SINK_H


#include "ComputeSink.h"
#include "TupleSetMachine.h"
#include "TupleSet.h"
#include "DataTypes.h"
#include "StorageClient.h"
#include <vector>

namespace pdb {

// runs hashes all of the tuples, and stores all tuples to a container that is partitioned
// by node partitions.
template <class KeyType, class ValueType>
class HashPartitionSink : public ComputeSink {


public:

    /**
     * constructor.
     * @param numPartitions: number of partitions in the cluster
     * @param inputSchema: the schema of input tuple set
     * @param attToOperateOn: the column that we want to partition and keep
     * @param storeConflictingObjectsOrNot: whether to store conflicting objects
     * @param port: port of PDB server on current node
     */
    HashPartitionSink(int numPartitions, int numNodes, TupleSpec& inputSchema, TupleSpec& attsToOperateOn, bool storeConflictingObjectsOrNot = false, int myNodeId = 0, bool recoverData = false, Handle<Vector<int>> nodesToRecover = nullptr) {

        // to setup the output tuple set
        TupleSpec empty;
        TupleSetSetupMachine myMachine(inputSchema, empty);

        // this is the input attribute that we will process
        std::vector<int> matches = myMachine.match(attsToOperateOn);
        whichAttToStore = matches[0];
        whichAttToHash = matches[1];
        std::cout << "whichAttToStore=" << whichAttToStore << std::endl;
        std::cout << "whichAttToHash=" << whichAttToHash << std::endl;
        this->numPartitions = numPartitions;
        this->numNodes = numNodes;
        std::cout << "numPartitions=" << numPartitions << std::endl;
        std::cout << "numNodes=" << numNodes << std::endl;

        this->storeConflictingObjects = storeConflictingObjectsOrNot;
        this->myNodeId = myNodeId;

        for (int i = 0; i < numNodes; i++) {
            nodeStatus.push_back(false);
        }
        this->recoverData = recoverData;
        if ((recoverData == true) && (nodesToRecover != nullptr)) {
            for (size_t i = 0; i < nodesToRecover->size(); i++) {
                nodeStatus[((*nodesToRecover)[i])] = true;
            }       
        }

    }

    /**
     * create container for output
     * @return: a new container for partitioned output
     */
    Handle<Object> createNewOutputContainer() override {

        // we create a node-partitioned vector to store the output
        Handle<Vector<Handle<Vector<Handle<ValueType>>>>> returnVal =
            makeObject<Vector<Handle<Vector<Handle<ValueType>>>>>(numNodes);
        for (int i = 0; i < numNodes; i++) {
            Handle<Vector<Handle<ValueType>>> curNodeVec 
                = makeObject<Vector<Handle<ValueType>>>();
            returnVal->push_back(curNodeVec);
        }
        return returnVal;
    }


    /**
     * partition the input tuple set, and store the partitioned result to the output container
     * @param input: the input tuple set
     * @param writeToMe: the output container
     */

    void writeOut(TupleSetPtr input, Handle<Object>& writeToMe) override {

        // get the partitioned vector we are adding to
        Handle<Vector<Handle<Vector<Handle<ValueType>>>>> writeMe =
            unsafeCast<Vector<Handle<Vector<Handle<ValueType>>>>>(writeToMe);
        size_t hashVal;


        // get the key columns
        std::vector<KeyType>& keyColumn = input->getColumn<KeyType>(whichAttToHash);

        // get the value columns
        std::vector<Handle<ValueType>>& valueColumn = input->getColumn<Handle<ValueType>>(whichAttToStore);

        // and allocate everyone to a partition
        size_t length = keyColumn.size();
        for (size_t i = 0; i < length; i++) {

            hashVal = Hasher<KeyType>::hash(keyColumn[i]);
            int nodeId = (hashVal % (numPartitions))/(numPartitions/numNodes);
            if ((nodeId == myNodeId) && storeConflictingObjects) {
                if ((!recoverData) || ((recoverData)&&(nodeStatus[nodeId] == true))) {
                    Vector<Handle<ValueType>>& myVec = *((*writeMe)[nodeId]);
                    try {
                       //to add the value to the partition
                        myVec.push_back(valueColumn[i]);

                    } catch (NotEnoughSpace & n) {

                       /* if we got here then we run out of space and we need delete the already-processed
                        *  data, throw an exception so that new space can be allocated by handling the exception,
                        *  and try to process the remaining unprocessed data again */
                        keyColumn.erase(keyColumn.begin(), keyColumn.begin() + i);
                        valueColumn.erase(valueColumn.begin(), valueColumn.begin() + i);
                        throw n;

                    }
                }
            }
            if (! storeConflictingObjects) {
                if ((!recoverData) || ((recoverData)&&(nodeStatus[nodeId] == true))) {
                    Vector<Handle<ValueType>>& myVec = *((*writeMe)[nodeId]);

                    try {
                        //to add the value to the partition
                        myVec.push_back(valueColumn[i]);

                    } catch (NotEnoughSpace & n) {

                        /* if we got here then we run out of space and we need delete the already-processed
                         *  data, throw an exception so that new space can be allocated by handling the exception,
                         *  and try to process the remaining unprocessed data again */
                        keyColumn.erase(keyColumn.begin(), keyColumn.begin() + i);
                        valueColumn.erase(valueColumn.begin(), valueColumn.begin() + i);
                        throw n;

                   }
                }
            }


        }
    }

    ~HashPartitionSink() {}


private:
    // the attribute to operate on
    int whichAttToHash;

    // the attribute to store
    int whichAttToStore;

    // number of partitions in the cluster
    int numPartitions;

    // number of nodes in the cluster
    int numNodes;

    // whether to detect and store conflicting objects for heterogeneous replication in case of failure recovery
    bool storeConflictingObjects = false;

    // the node Id of current node
    int myNodeId = 0;

    // whether to recover data
    bool recoverData = false;

    // the node Id to recover;
    Vector<bool> nodeStatus;
};
}

#endif
