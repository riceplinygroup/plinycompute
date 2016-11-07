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
#ifndef PDB_TCAPPARSER_TABLEASSIGNMENT_H
#define PDB_TCAPPARSER_TABLEASSIGNMENT_H

#include <memory>
#include <vector>

#include "TcapStatement.h"
#include "TableAssignment.h"
#include "TableExpression.h"

using std::shared_ptr;
using std::vector;

namespace pdb_detail
{
    class TableAssignment : public TcapStatement
    {
    public:

        TcapIdentifier tableName;

        shared_ptr<vector<TcapIdentifier>> columnNames;

        shared_ptr<TableExpression> value;

        TableAssignment(TcapIdentifier tableName, shared_ptr<vector<TcapIdentifier>> columnNames,  shared_ptr<TableExpression> value);

        virtual void match(function<void(TableAssignment &)> forTableAssignment,
                           function<void(StoreOperation &)> forStore);

    };

}

#endif //PDB_TCAPPARSER_TABLEASSIGNMENT_H
