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
#ifndef PDB_TCAPINTERMEDIARYREP_COLUMN_H
#define PDB_TCAPINTERMEDIARYREP_COLUMN_H

#include <string>

using std::string;

namespace pdb_detail {
/**
 * Identifies a table and column.
 */
class TableColumn {
public:
    /**
     * The table.
     */
    const string tableId;

    /**
     * The column.
     */
    const string columnId;

    /**
     * Creates a new TableColumn.
     *
     * @param tableId the table
     * @param columnId the column
     * @return a new TableColumn.
     */
    TableColumn(const string& tableId, const string& columnId);
};
}

#endif  // PDB_TCAPINTERMEDIARYREP_COLUMN_H
