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
#include "ApplyOperation.h"

using std::invalid_argument;
using std::make_shared;

namespace pdb_detail
{
    ApplyOperation::ApplyOperation(string applyTarget, ApplyOperationType applyType, TcapIdentifier inputTable,
                                   shared_ptr<vector<TcapIdentifier>> inputTableColumnNames,
                                   shared_ptr<RetainClause> retain)
            : applyTarget(applyTarget), applyType(applyType),  inputTable(inputTable),
              inputTableColumnNames(inputTableColumnNames),
              retain(retain)
    {
        if(inputTableColumnNames == nullptr)
            throw new std::invalid_argument("inputTableColumnNames may not be null");

        if(retain == nullptr)
            throw new std::invalid_argument("retain may not be null");
    }


    void ApplyOperation::match(function<void(LoadOperation&)>, function<void(ApplyOperation&)> forApply,
                               function<void(FilterOperation&)>,
                               function<void(HoistOperation&)>, function<void(BinaryOperation&)>)
    {
        forApply(*this);
    }
}