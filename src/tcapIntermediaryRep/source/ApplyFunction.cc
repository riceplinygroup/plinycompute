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
#include "ApplyFunction.h"

namespace pdb_detail
{

    ApplyFunctionPtr makeApplyFunction(string executorId, string functionId, string outputTableId, string outputColumnId,
                      TableColumns inputColumns, shared_ptr<vector<Column>> columnsToCopyToOutputTable)
    {
        return make_shared<ApplyFunction>(executorId, functionId, outputTableId, outputColumnId, inputColumns,
                                          columnsToCopyToOutputTable);
    }


    void ApplyFunction::match(function<void(Load&)>, function<void(ApplyFunction&)> forApplyFunc, function<void(ApplyMethod&)>,
               function<void(Filter&)>, function<void(Hoist&)>, function<void(GreaterThan&)>,
               function<void(Store&)> forStore)
    {
        forApplyFunc(*this);
    }
}