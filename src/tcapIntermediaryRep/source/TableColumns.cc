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
#include "TableColumns.h"

namespace pdb_detail
{

    TableColumns::TableColumns(string tableName, shared_ptr<vector<string>> columnIds)
                : tableName(tableName), columnIds(columnIds)
        {
        }

    TableColumns::TableColumns(string tableName, string columnId)
                : tableName(tableName), columnIds(make_shared<vector<string>>())
        {
            columnIds->push_back(columnId);
        }

    TableColumns::TableColumns(string tableName, string columnId1, string columnId2)
                : tableName(tableName), columnIds(make_shared<vector<string>>())
        {
            columnIds->push_back(columnId1);
            columnIds->push_back(columnId2);
        }

    string TableColumns::operator[](vector<string>::size_type index) const
    {
        return columnIds->operator[](index);
    }
}
