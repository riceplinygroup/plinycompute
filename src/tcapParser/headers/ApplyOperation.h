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
#ifndef PDB_TCAPPARSER_APPLYOPERATION_H
#define PDB_TCAPPARSER_APPLYOPERATION_H

#include <memory>
#include <string>
#include <vector>

#include "RetainClause.h"
#include "TableExpression.h"
#include "TcapIdentifier.h"

using std::shared_ptr;
using std::string;
using std::vector;

namespace pdb_detail
{
    enum ApplyOperationType
    {
        func, method
    };

    class ApplyOperation : public TableExpression
    {
    public:

        shared_ptr<string> applyTarget;

        ApplyOperationType applyType;

        TcapIdentifier inputTable;

        shared_ptr<vector<TcapIdentifier>> inputTableColumnNames;

        shared_ptr<RetainClause> retain;

        ApplyOperation(string applyTarget, ApplyOperationType applyType, TcapIdentifier inputTable, shared_ptr<vector<TcapIdentifier>> inputTableColumnNames,
                       shared_ptr<RetainClause> retain);

        ApplyOperation(shared_ptr<string> applyTarget, ApplyOperationType applyType, TcapIdentifier inputTable,
                       shared_ptr<vector<TcapIdentifier>> inputTableColumnNames, shared_ptr<RetainClause> retain);

        void execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)> forApply, function<void(FilterOperation&)>,
                     function<void(HoistOperation&)>, function<void(BinaryOperation&)>);
    };
}

#endif //PDB_TCAPPARSER_APPLYOPERATION_H
