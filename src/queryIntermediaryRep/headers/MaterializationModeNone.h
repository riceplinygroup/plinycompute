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
#ifndef PDB_QUERYINTERMEDIARYREP_MATERIALIZATIONMODENONE_H
#define PDB_QUERYINTERMEDIARYREP_MATERIALIZATIONMODENONE_H

#include <string>

#include "MaterializationMode.h"

using std::string;

namespace pdb_detail {
/**
* Signifies no materilization is to be done.
*/
class MaterializationModeNone : public MaterializationMode {

public:
    /**
     * @return true
     */
    bool isNone() override;

    // contract from super
    void execute(MaterializationModeAlgo& algo) override;

    // contract from super
    string tryGetDatabaseName(const string& noneValue) override;

    // contract from super
    string tryGetSetName(const string& noneValue) override;
};
}

#endif  // PDB_QUERYINTERMEDIARYREP_MATERIALIZATIONMODENONE_H
