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
//
// Created by Joseph Hwang on 9/25/16.
//

#ifndef OBJECTQUERYMODEL_DISPATCHERREGISTERPARTITIONPOLICY_H
#define OBJECTQUERYMODEL_DISPATCHERREGISTERPARTITIONPOLICY_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

#include "PartitionPolicy.h"

// PRELOAD %DispatcherRegisterPartitionPolicy%

namespace pdb {

class DispatcherRegisterPartitionPolicy : public Object {

public:

    DispatcherRegisterPartitionPolicy() {}
    ~DispatcherRegisterPartitionPolicy() {}

    DispatcherRegisterPartitionPolicy(std::string setNameIn, std::string databaseNameIn, PartitionPolicy::Policy policyIn) :
            setName(setNameIn), databaseName(databaseNameIn), policy(policyIn) {}

    String getSetName() {
        return this->setName;
    }

    String getDatabaseName() {
        return this->databaseName;
    }

    PartitionPolicy::Policy getPolicy() {
        return this->policy;
    }

    ENABLE_DEEP_COPY

private:

    String setName;
    String databaseName;
    PartitionPolicy::Policy policy;

};

}


#endif //OBJECTQUERYMODEL_DISPATCHERREGISTERPARTITIONPOLICY_H
