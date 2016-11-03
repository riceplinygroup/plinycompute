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
#include "Hoist.h"

using std::make_shared;

namespace pdb_detail
{

    HoistPtr makeHoist(string fieldId, Column inputColumn, Column outputColumn,
                       shared_ptr<vector<Column>> columnsToCopyToOutputTable, string executorId)
    {
        return make_shared<Hoist>(fieldId, inputColumn, outputColumn, columnsToCopyToOutputTable, executorId);
    }
}