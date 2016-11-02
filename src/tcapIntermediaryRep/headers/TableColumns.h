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

using std::make_shared;
using std::shared_ptr;
using std::string;
using std::vector;

namespace pdb_detail
{
    /**
     * columnNames may not be empty.
     */
    class TableColumns
    {
    public:

        const string tableName;

        const shared_ptr<vector<string>> columnIds;

        TableColumns(string tableName, shared_ptr<vector<string>> columnIds)
            : tableName(tableName), columnIds(columnIds)
        {
        }

        TableColumns(string tableName, string columnId)
                : tableName(tableName), columnIds(make_shared<vector<string>>())
        {
            columnIds->push_back(columnId);
        }

        TableColumns(string tableName, string columnId1, string columnId2)
                : tableName(tableName), columnIds(make_shared<vector<string>>())
        {
            columnIds->push_back(columnId1);
            columnIds->push_back(columnId2);
        }

        string operator[](vector<string>::size_type index) const
        {
            return columnIds->operator[](index);
        }

    };
}

#endif //PDB_TCAPINTERMEDIARYREP_TABLECOLUMNS_H
