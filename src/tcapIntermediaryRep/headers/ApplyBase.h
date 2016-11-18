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
#ifndef PDB_TCAPINTERMEDIARYREP_APPLYBASE_H
#define PDB_TCAPINTERMEDIARYREP_APPLYBASE_H

#include <memory>
#include <vector>

#include "TableColumn.h"
#include "Instruction.h"
#include "TableColumns.h"

using std::shared_ptr;
using std::vector;

using pdb_detail::TableColumn;

namespace pdb_detail
{
    /**
     * Base class for "apply" TCAP operations that apply a function (or method) to some table to produce
     * an output colunn.
     */
    class ApplyBase : public Instruction
    {

    public:

        /**
         * An identifier specifying the executor to be used to create the output column.
         */
        const string executorId;

        /**
         * A metadata value describing the origin of executorId.
         */
        const string functionId;

        /**
         * The id of the table to be created by application of the instruction.
         */
        const string outputTableId;

        /**
         * The id of the column in the created output table where the executor's output is to be recorded.
         */
        const string outputColumnId;

        /**
         * The input arguments to the executor.
         */
        const TableColumns inputColumns;

        /**
         * Any optional columns to copy into the output table during its contruction.
         */
        const shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable;

        /**
         * @return the number of coumns in outputTableId created by executing the instruction.
         */
        uint getOutputTableColumnCount();

    protected:

        /**
         * Creates a new ApplyFunction instruction.
         *
         * @param executorId the name of the executor to be applied
         * @param functionId any metadata value describing the origin of the executor
         * @param outputTableId the id of the table to be created by execution of the instruction
         * @param outputColumnId the column in the output table to store the executor's output.
         * @param inputColumns the input columns to the executor. May not be empty.
         * @param columnsToCopyToOutputTable  any columns that should be copied into the output table
         */
        ApplyBase(const string &executorId, const string &functionId, const string &outputTableId,
                  const string &outputColumnId, TableColumns inputColumns, shared_ptr<vector<TableColumn>>
                  columnsToCopyToOutputTable, InstructionType type);



    };
}

#endif //PDB_TCAPINTERMEDIARYREP_APPLYFUNCTION_H
