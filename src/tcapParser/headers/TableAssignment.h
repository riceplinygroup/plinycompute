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
    /**
     * Models an assigment from a TableExpression to a new table.
     */
    class TableAssignment : public TcapStatement
    {
    public:

        /**
         * The name of the table to create.
         */
        const TcapIdentifier tableName;

        /**
         * The columns of the new table.
         */
        const shared_ptr<const vector<TcapIdentifier>> columnNames;

        /**
         * The value to be assigned to the new table.
         */
        const shared_ptr<TableExpression> value;

        /**
         * Creeates a new assigment.
         *
         * If columnNames is nullptr, throws invalid_argument exception.
         *
         * @param tableName The name of the table to create.
         * @param columnNames The columns of the new table.
         * @param value The value to be assigned to the new table.
         * @return a new TableAssignment
         */
        TableAssignment(TcapIdentifier tableName, shared_ptr<vector<TcapIdentifier>> columnNames,
                        shared_ptr<TableExpression> value);

        /**
         * Creeates a new assigment with a sole attribute.
         *
         * If columnNames is nullptr, throws invalid_argument exception.
         *
         * @param attribute an attribute for the statement.
         * @param tableName The name of the table to create.
         * @param columnNames The columns of the new table.
         * @param value The value to be assigned to the new table.
         * @return a new TableAssignment
         */
        TableAssignment(TcapAttribute attribute, TcapIdentifier tableName,
                        shared_ptr<vector<TcapIdentifier>> columnNames, shared_ptr<TableExpression> value);

        /**
         * Creeates a new assigment with the given attributes.
         *
         * @param attributes the attributes of the statement.
         * @param tableName The name of the table to create.
         * @param columnNames The columns of the new table.
         * @param value The value to be assigned to the new table.
         * @return a new TableAssignment
         */
        TableAssignment(shared_ptr<vector<TcapAttribute>> attributes, TcapIdentifier tableName,
                        shared_ptr<vector<TcapIdentifier>> columnNames, shared_ptr<TableExpression> value);

        // contract from super
        virtual void match(function<void(TableAssignment &)> forTableAssignment,
                           function<void(StoreOperation &)> forStore);

    };

    typedef shared_ptr<TableAssignment> TableAssignmentPtr;

}

#endif //PDB_TCAPPARSER_TABLEASSIGNMENT_H
