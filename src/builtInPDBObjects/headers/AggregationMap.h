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
#ifndef AGGREGATION_MAP_H
#define AGGREGATION_MAP_H

// PRELOAD %AggregationMap<Nothing>%


#include "PDBMap.h"

namespace pdb {

// This is the AggregationMap type that serves as a map for aggregation

template <class KeyType, class ValueType = Nothing>
class AggregationMap : public Map<KeyType, ValueType> {

private:
    // this member will only be used in PDB aggregation
    unsigned int hashPartitionId;


public:
    ENABLE_DEEP_COPY


    // this constructor pre-allocates initSize slots... initSize must be a power of two
    AggregationMap(uint32_t initSize);

    // this constructor creates a map with a single slot
    AggregationMap();

    // destructor
    ~AggregationMap();

    // enhance the performance of indexing a hash partition.

    unsigned int getHashPartitionId();
    void setHashPartitionId(unsigned int id);
};
}

#include "AggregationMap.cc"

#endif
