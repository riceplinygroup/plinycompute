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
#ifndef PDB_TCAPINTERMEDIARYREP_TABLECOLUMNS_H
#define PDB_TCAPINTERMEDIARYREP_TABLECOLUMNS_H

#include <memory>
#include <string>
#include <vector>

#include "BuildLogicalPlanTests.h"
#include "Instruction.h"

using std::make_shared;
using std::shared_ptr;
using std::string;
using std::vector;

namespace pdb_detail
{
    /**
     * A list of columns within a table.
     */
    class TableColumns
    {
    public:

        /**
         * The name of the table.
         */
        const string tableName;

        /**
         * A non-empty list of unique columnIds.
         */
        const shared_ptr<const vector<string>> columnIds;

        /**
         * Creates a new TableColumns of one column.
         *
         * @param tableName The name of the table.
         * @param columnId the only column for columnIds
         * @return  a new TableColumns
         */
        TableColumns(const string &tableName, const string &columnId);

        /**
         * Creates a new TableColumns of two columns in the order columnId1, columnId2.
         *
         * If columnId1 == columnId2, columnIds is set to nullptr;
         *
         * @param tableName The name of the table.
         * @param columnId1 the fisrt column
         * @param columnId2 the second column
         * @return  a new TableColumns
         */
        TableColumns(const string &tableName, const string &columnId1, const string &columnId2);

        /**
         * @return columnIds->operator[](index)
         */
        string operator[](vector<string>::size_type index) const;

    private:

        /**
         * Creates a new TableColumns.
         *
         * If columnIds is empty or contains repeated strings, columnIds is set to nullptr.
         *
         * @param tableName The name of the table.
         * @param columnIds A non-empty list of unique columnIds.
         * @return a new TableColumns
         */
        TableColumns(const string &tableName, shared_ptr<vector<string>> columnIds);

        friend void pdb_tests::testBuildLogicalPlanFromStore(class UnitTest &qunit);

        friend InstructionPtr makeInstructionFromApply(const class ApplyOperation &applyOp,
                                                       const class TableAssignment& tableAssignment);

        friend shared_ptr<class Store> makeInstructionFromStore(const class StoreOperation &storeOperation);

    };
}

#endif //PDB_TCAPINTERMEDIARYREP_TABLECOLUMNS_H
