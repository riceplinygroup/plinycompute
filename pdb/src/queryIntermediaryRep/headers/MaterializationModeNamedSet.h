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
#ifndef PDB_QUERYINTERMEDIARYREP_MATERIALIZATIONMODENAMEDSET_H
#define PDB_QUERYINTERMEDIARYREP_MATERIALIZATIONMODENAMEDSET_H

#include <string>

#include "MaterializationMode.h"

namespace pdb_detail {

/**
 * Signifies that materilization is to be performed to a specific database name and set name.
 */
class MaterializationModeNamedSet : public MaterializationMode {

public:
    /**
     * Creates a new MaterializationModeNamedSet.
     *
     * @param databaseName he database name of the materilization destination
     * @param setName the set name of the materilization destination
     * @return a new MaterializationModeNamedSet
     */
    MaterializationModeNamedSet(string databaseName, string setName);

    /**
     * @return false
     */
    bool isNone() override;

    // contract from super
    void execute(MaterializationModeAlgo& algo) override;

    /**
     * @return the database name of the materilization destination
     */
    string getDatabaseName();

    /**
     * @return the set name of the materilization destination
     */
    string getSetName();

    /**
     * @param noneValue ignored
     * @return the database name of the materilization destination
     */
    string tryGetDatabaseName(const string& noneValue) override;

    /**
     * @param noneValue ignored
     * @return the set name of the materilization destination
     */
    string tryGetSetName(const string& noneValue) override;

private:
    string _databaseName;

    string _setName;
};
}

#endif  // PDB_QUERYINTERMEDIARYREP_MATERIALIZATIONMODENAMEDSET_H
