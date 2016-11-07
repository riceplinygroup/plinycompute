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
#include "HoistOperation.h"

using std::make_shared;

namespace pdb_detail
{
    HoistOperation::HoistOperation(string hoistTarget, TcapIdentifier inputTable, shared_ptr<vector<TcapIdentifier>> inputTableColumnNames,
                                   shared_ptr<RetainClause> retain)
            : HoistOperation(make_shared<string>(hoistTarget), inputTable, inputTableColumnNames, retain)
    {
    }

    HoistOperation::HoistOperation(shared_ptr<string> hoistTarget, TcapIdentifier inputTable,
                                   shared_ptr<vector<TcapIdentifier>> inputTableColumnNames, shared_ptr<RetainClause> retain)
            : hoistTarget(hoistTarget), inputTable(inputTable), inputTableColumnNames(inputTableColumnNames),
              retain(retain)
    {
    }

    void HoistOperation::execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)>, function<void(FilterOperation&)>,
                                 function<void(HoistOperation&)> forHoist, function<void(BinaryOperation&)> forBinaryOp)
    {
        return forHoist(*this);
    }

}