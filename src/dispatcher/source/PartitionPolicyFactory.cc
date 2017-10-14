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
#ifndef OBJECTQUERYMODEL_PARTITIONPOLICYFACTORY_CC
#define OBJECTQUERYMODEL_PARTITIONPOLICYFACTORY_CC

#include "PartitionPolicyFactory.h"

namespace pdb {

PartitionPolicyPtr PartitionPolicyFactory::buildPartitionPolicy(PartitionPolicy::Policy policy) {
    switch (policy) {
        case PartitionPolicy::Policy::RANDOM:
            return std::make_shared<RandomPolicy>();
        case PartitionPolicy::Policy::ROUNDROBIN:
            return std::make_shared<RoundRobinPolicy>();
        case PartitionPolicy::Policy::FAIR:
            // TODO: Implement this class
            return nullptr;
        case PartitionPolicy::Policy::DEFAULT:
            // Random policy is the default policy
            return buildDefaultPartitionPolicy();
    }
}

PartitionPolicyPtr PartitionPolicyFactory::buildDefaultPartitionPolicy() {
#ifdef RANDOM_DISPATCHER
    return std::make_shared<RandomPolicy>();
#else
    return std::make_shared<RoundRobinPolicy>();
#endif
}
}

#endif
