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
#include "GreaterThan.h"

using std::make_shared;

namespace pdb_detail
{

    GreaterThan::GreaterThan(Column leftHandSide, Column rightHandSide, Column outputColumn,
    shared_ptr<vector<Column>> columnsToCopyToOutputTable, string executorId)

    : Instruction(InstructionType::greater_than), leftHandSide(leftHandSide),
    rightHandSide(rightHandSide), outputColumn(outputColumn),
    columnsToCopyToOutputTable(columnsToCopyToOutputTable), executorId(executorId)
    {
    }

    void GreaterThan::match(function<void(Load&)> forLoad, function<void(ApplyFunction&)>, function<void(ApplyMethod&)>,
               function<void(Filter&)>, function<void(Hoist&)>, function<void(GreaterThan&)> forGreaterThan,
               function<void(Store&)>)
    {
        forGreaterThan(*this);
    }

    GreaterThanPtr makeGreaterThan(Column leftHandSide, Column rightHandSide, Column outputColumn,
                                   shared_ptr<vector<Column>> columnsToCopyToOutputTable, string executorId)
    {
        return make_shared<GreaterThan>(leftHandSide, rightHandSide, outputColumn, columnsToCopyToOutputTable, executorId);
    }
}