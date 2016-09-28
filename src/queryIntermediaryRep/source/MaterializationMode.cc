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
#include "MaterializationMode.h"

using std::string;

namespace pdb_detail
{
    bool MaterializationModeNone::isNone()
    {
        return true;
    }


    MaterializationModeNamedSet::MaterializationModeNamedSet(string databaseName, string setName)
            : _databaseName(databaseName), _setName(setName)
    {
    }

    bool MaterializationModeNamedSet::isNone()
    {
        return false;
    }

    string MaterializationModeNamedSet::getDatabaseName()
    {
        return _databaseName;
    }

    string MaterializationModeNamedSet::getSetName()
    {
        return _setName;
    }


}