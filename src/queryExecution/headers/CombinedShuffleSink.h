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

#ifndef COMBINED_SHUFFLE_SINK_H
#define COMBINED_SHUFFLE_SINK_H

//by Jia, Mar 2017

#include "ComputeSink.h"
#include "TupleSetMachine.h"
#include "TupleSet.h"
#include "DataTypes.h"
#include "AggregationMap.h"
#include <vector>

namespace pdb {

// runs hashes all of the tuples, and stores aggregated results to a container that is partitioned by node partitions.
template <class KeyType, class ValueType>
class CombinedShuffleSink : public ComputeSink {

private:

	// the attributes to operate on
	int whichAttToHash;
	int whichAttToAggregate;
        int numNodes;
        int numPartitionsPerNode;

public:

        CombinedShuffleSink (int numPartitionsPerNode, int numNodes, TupleSpec &inputSchema, TupleSpec &attsToOperateOn) {


                // to setup the output tuple set
                TupleSpec empty;
                TupleSetSetupMachine myMachine (inputSchema, empty);

                // this is the input attribute that we will process
                std :: vector <int> matches = myMachine.match (attsToOperateOn);
                whichAttToHash = matches[0];
                whichAttToAggregate = matches[1];
                this->numNodes = numNodes;
                this->numPartitionsPerNode = numPartitionsPerNode;

        }



        AggregationMap<KeyType, ValueType> & getMap (HashPartitionID myHashID, Handle <Vector<Handle<Vector <Handle<AggregationMap<KeyType, ValueType>>>>>> outputData) {

                int nodeId = myHashID / numPartitionsPerNode;
                int partitionId = myHashID % numPartitionsPerNode;
                return *((*((*outputData)[nodeId]))[partitionId]);

        }

	Handle <Object> createNewOutputContainer () override {

		// we create a node-partitioned map to store the output
		Handle <Vector<Handle<Vector <Handle<AggregationMap<KeyType, ValueType>>>>>> returnVal = makeObject <Vector<Handle<Vector<Handle<AggregationMap <KeyType, ValueType>>>>>> (numNodes);
                int i, j;
                for (i = 0; i < numNodes; i++) {
                    Handle<Vector<Handle<AggregationMap<KeyType, ValueType>>>> nodeData = makeObject<Vector<Handle<AggregationMap<KeyType, ValueType>>>> (this->numPartitionsPerNode);
                    for (j = 0; j < this->numPartitionsPerNode; j ++) {
                        Handle<AggregationMap<KeyType, ValueType>> curMap = makeObject <AggregationMap <KeyType, ValueType>>();
                        curMap->setHashPartitionId(j);
                        nodeData->push_back(curMap);
                    }
                    returnVal->push_back(nodeData);
                }
		return returnVal;	
	}

	void writeOut (TupleSetPtr input, Handle <Object> &writeToMe) override {

		// get the map we are adding to
		Handle <Vector <Handle <Vector <Handle <AggregationMap <KeyType, ValueType>>>>>> writeMe = unsafeCast <Vector<Handle <Vector <Handle <AggregationMap <KeyType, ValueType>>>>>> (writeToMe);
                size_t hashVal;  
               

		// get the input columns
		std :: vector <KeyType> &keyColumn = input->getColumn <KeyType> (whichAttToHash);
		std :: vector <ValueType> &valueColumn = input->getColumn <ValueType> (whichAttToAggregate);

		// and aggregate everyone
		size_t length = keyColumn.size ();
		for (size_t i = 0; i < length; i++) {

                        hashVal = Hasher <KeyType> :: hash(keyColumn[i]);

                        AggregationMap <KeyType, ValueType> & myMap = getMap(hashVal%(numNodes * numPartitionsPerNode), writeMe);
			// if this key is not already there...
			if (myMap.count (keyColumn[i]) == 0) {

				// this point will record where the value is located
				ValueType *temp = nullptr;

				// try to add the key... this will cause an allocation for a new key/val pair
				try {
					// get the location that we need to write to...
					temp = &(myMap[keyColumn[i]]);

				// if we get an exception, then we could not fit a new key/value pair
				} catch (NotEnoughSpace &n) {
					// if we got here, then we ran out of space, and so we need to delete the already-processed
                                        //std :: cout << "not enough space in shuffle sink to get new value" << std :: endl;
					// data so that we can try again...
					keyColumn.erase (keyColumn.begin (), keyColumn.begin () + i);
					valueColumn.erase (valueColumn.begin (), valueColumn.begin () + i);
					throw n;
				}

				// we were able to fit a new key/value pair, so copy over the value
				try {
					*temp = valueColumn[i];
				// if we could not fit the value...
				} catch (NotEnoughSpace &n) {

                                        //std :: cout << "not enough space in shuffle sink to set value" << std :: endl;
					// then we need to erase the key from the map
					myMap.setUnused (keyColumn[i]);

					// and erase all of these guys from the tuple set since they were processed
					keyColumn.erase (keyColumn.begin (), keyColumn.begin () + i);
					valueColumn.erase (valueColumn.begin (), valueColumn.begin () + i);
					throw n;
				}

			// the key is there
			} else {
				
				// get the value and a copy of it
				ValueType &temp = myMap[keyColumn[i]];
				ValueType copy = temp;

				// and add to the old value, producing a new one
				try {
					temp = copy + valueColumn[i];
				// if we got here, then it means that we ram out of RAM when we were trying 
				// to put the new value into the hash table
				} catch (NotEnoughSpace &n) {	
					
                                        //std :: cout << "not enough space in shuffle sink to update value" << std :: endl;
					// restore the old value
					temp = copy;

					// and erase all of the guys who were processed
					keyColumn.erase (keyColumn.begin (), keyColumn.begin () + i);
					valueColumn.erase (valueColumn.begin (), valueColumn.begin () + i);
					throw n;
				}
			}
		}

	}

	~CombinedShuffleSink () {}
};

}

#endif
