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

#include <set>

using std::set;
using std::invalid_argument;

namespace pdb_detail
{
    /**
     * Checks if columnIds is empty or contains duplicates.
     *
     * @param columnIds the list to check
     * @return columnIds if non-empty and contains no duplicates, else nullptr
     */
    shared_ptr<vector<string>> makeColumnIdVector(shared_ptr<vector<string>> columnIds)
    {
        if(columnIds->size() == 0)
            return nullptr;

        set<string> seenIds;

        for(string id : *columnIds.get())
        {
            if(seenIds.count(id) > 0)
                return nullptr;
        }

        return columnIds;
    }

    /**
     * @return a vector with only id as a member.
     */
    shared_ptr<vector<string>> makeColumnIdVector(const string &id)
    {
        shared_ptr<vector<string>> ids = make_shared<vector<string>>();
        ids->push_back(id);
        return ids;
    }

    /**
     * @return the vector [id1, id2] iff id1 != id2, else nullptr
     */
    shared_ptr<vector<string>> makeColumnIdVector(const string &id1, const string &id2)
    {
        if(id1 == id2)
            return nullptr;

        shared_ptr<vector<string>> ids = make_shared<vector<string>>();
        ids->push_back(id1);
        ids->push_back(id2);
        return ids;
    }


    TableColumns::TableColumns(const string &tableName, shared_ptr<vector<string>> columnIds)
                : tableName(tableName), columnIds(makeColumnIdVector(columnIds))
        {
            if(columnIds == nullptr)
                throw invalid_argument("columnIds may not be null");
        }

    TableColumns::TableColumns(const string &tableName, const string &columnId)
                : tableName(tableName), columnIds(makeColumnIdVector(columnId))
        {
        }

    TableColumns::TableColumns(const string &tableName, const string &columnId1, const string &columnId2)
                : tableName(tableName), columnIds(makeColumnIdVector(columnId1, columnId2))
        {
        }

    string TableColumns::operator[](vector<string>::size_type index) const
    {
        return columnIds->operator[](index);
    }
}
