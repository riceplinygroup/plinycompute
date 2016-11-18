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
#ifndef PDB_TCAPINTERMEDIARYREP_FILTER_H
#define PDB_TCAPINTERMEDIARYREP_FILTER_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "ApplyFunction.h"
#include "ApplyMethod.h"
#include "Filter.h"
#include "Hoist.h"
#include "TableColumn.h"
#include "Load.h"

using std::function;
using std::shared_ptr;
using std::string;
using std::vector;

using pdb_detail::TableColumn;

namespace pdb_detail
{

    /**
     * An instruction that describes how to create a new table from an existing table by filtering the rows of the
     * input table based on a boolean value in each row.
     *
     * For example, for the input table "Input"
     *
     * ----------|----------
     *   Num     |   isOdd
     * ----------|----------
     *     2     |    false
     * ----------|----------
     *     7     |    true
     * ----------|----------
     *
     * a filter operation could consider the column isOdd to produce an output table "Output" of the form:
     *
     *
     * ----------|----------
     *   Num     |   isOdd
     * ----------|----------
     *     7     |    true
     * ----------|----------
     *
     *
     * In this example the filter instruction that described creation of the output table above would have the
     * following form:
     *
     * inputTableId would be "Input" to encode that the input table was the source of consideration.
     *
     * filterColumnId would be "isOdd" to encode that row should be retained or disguarded based on their value
     * in the isOdd column.
     *
     * columnsToCopyToOutputTable would have the value [("Input", "Num"), ("Input", "isOdd")] to encode that
     * Output should contain copies of the Num and IsOdd columns from the input table.
     *
     * outputTableId would have the value "Output" to encode the name of the created table.
     */
    class Filter : public Instruction
    {

    public:

        /**
         * The id of the table to be created by application of the instruction.
         */
        const string inputTableId;

        /**
         * The namme of the column in the input table on which the filter operation operates.
         */
        const string filterColumnId;

        /**
         * The id of the table to be created by application of the instruction.
         */
        const string outputTableId;

        /**
         * Any optional columns to copy into the output table during its contruction.
         */
        const shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable;

        // contract from super
        void match(function<void(Load&)>, function<void(ApplyFunction&)>, function<void(ApplyMethod&)>,
                   function<void(Filter&)> forFilter, function<void(Hoist&)>, function<void(GreaterThan&)>,
                   function<void(Store&)>);

    private:

        /**
         * Creates a new filter instruction.
         *
         * If columnsToCopyToOutputTable is nullptr, throws invalid_argument exception.
         *
         * @param inputTableId the id of the table to be filtered
         * @param filterColumnId the boolean valued column in the input table to filter by
         * @param outputTableId the name of the result table
         * @param columnsToCopyToOutputTable any columns to copy from the output table to the input table
         * @return a new filter instruction
         */
        // private because throws exception and PDB style guide disallows exceptions from crossing API boundaries
        Filter(const string &inputTableId, const string &filterColumnId, const string &outputTableId,
               shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable);

        // for constructor
        friend shared_ptr<Filter> makeFilter(const string &inputTableId, const string &filterColumnId,
                                             const string &outputTableId,
                                             shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable);
        // for constructor
        friend InstructionPtr makeInstructionFromAssignment(const class TableAssignment& tableAssignment);

    };

    typedef shared_ptr<Filter> FilterPtr;

    /**
     * Creates a new filter instruction.
     *
     * If columnsToCopyToOutputTable is nullptr, returns nulltpr.
     *
     * @param inputTableId the id of the table to be filtered
     * @param filterColumnId the boolean valued column in the input table to filter by
     * @param outputTableId the name of the result table
     * @param columnsToCopyToOutputTable any columns to copy from the output table to the input table
     * @return a shared pointer to the new filter instruction
     */
    FilterPtr makeFilter(const string &inputTableId, const string &filterColumnId, const string &outputTableId,
                         shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable);
}

#endif //PDB_TCAPINTERMEDIARYREP_COLUMN_H
