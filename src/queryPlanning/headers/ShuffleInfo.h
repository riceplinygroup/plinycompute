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
#ifndef SHUFFLE_INFO_H
#define SHUFFLE_INFO_H

//by Jia, May 2017

#include "DataTypes.h"
#include "StandardResourceInfo.h"

namespace pdb {

class ShuffleInfo {

private:

    //number of nodes in this cluster
    int numNodes;

    //number of CPU cores allocated for hash aggregation and hash join in this cluster
    int numHashPartitions;

    //hash partition ids allocated for each node
    std :: vector < std :: vector <HashPartitionID>> partitionIds;

    //address for each node
    std :: vector < std :: string> addresses;

public:

    //constructor
    ShuffleInfo (std :: vector<StandardResourceInfoPtr> * clusterResources, double partitionToCoreRatio) {

        this->numNodes = clusterResources->size();
        this->numHashPartitions = 0;
        int i, j;
        HashPartitionID id = 0;
        partitionIds.resize(this->numNodes);
        for (i = 0; i < this->numNodes; i++) {
            StandardResourceInfoPtr node = clusterResources->at(i);
            int numCoresOnThisNodeForHashing = (int)((double)(node->getNumCores()) * partitionToCoreRatio);
            if (numCoresOnThisNodeForHashing == 0) {
                numCoresOnThisNodeForHashing = 1;
            }
            for (j = 0; j < numCoresOnThisNodeForHashing; j++) {
                partitionIds[i].push_back(id);
                id ++;
            }
            std :: string curAddress = node->getAddress() + ":" + std :: to_string(node->getPort());
            this->addresses.push_back(curAddress);
            this->numHashPartitions += numCoresOnThisNodeForHashing;
        }
       

    }

    ~ShuffleInfo () {
    }

    int getNumNodes () {
        return this->numNodes;
    }

    int getNumHashPartitions () {
        return this->numHashPartitions;
    }

    std :: vector < std :: vector < HashPartitionID > > & getPartitionIds () {
        return this->partitionIds;
    }

    std :: vector < std :: string> & getAddresses () {
        return this->addresses;
    }

};

}
#endif
