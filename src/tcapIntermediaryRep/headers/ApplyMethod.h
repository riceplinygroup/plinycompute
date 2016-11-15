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
#ifndef PDB_TCAPINTERMEDIARYREP_APPLYMETHOD_H
#define PDB_TCAPINTERMEDIARYREP_APPLYMETHOD_H

#include <memory>
#include <vector>
#include <sys/types.h>

#include "Column.h"
#include "ApplyBase.h"

using std::shared_ptr;
using std::vector;

namespace pdb_detail
{
    /**
     * An instruction that describes how to create a new table from an existing table by application of an
     * externally defined executor that's origin is a function.
     *
     * For example, consider an executor "sum" that consumes two integer columns to produce a new summation column.
     *
     * For the input table "Input"
     *
     * ----------|----------
     *   Alpha   |   Beta
     * ----------|----------
     *     2     |    4
     * ----------|----------
     *     1     |    2
     * ----------|----------
     *
     * sum could take columns Alpha and Beta to produce an output column named "Total" of the form:
     *
     *
     * |----------|
     * |  Total   |
     * |----------|
     * |    6     |
     * |----------|
     * |    3     |
     * |----------|
     *
     *
     * An ApplyFunction instruction could the be created to decribe how to take the table Input and produce
     * the following table Output (below) by means of both utilizing the sum executor and copying existing columns from
     * Input.
     *
     * ----------|----------|---------
     *   Alpha   |   Beta   |   Total
     * ----------|----------|---------
     *     2     |    4     |    6
     * ----------|----------|---------
     *     1     |    2     |    3
     * ----------|----------|---------
     *
     * In this example the apply instruction that described creation of the OutputTable above would have the
     * following form:
     *
     * executorId would be "sum" to encode that the sum executor should be used to create the
     * output collum.
     *
     * outputColumnId would be "Total" to encode that the output table should be named Total.
     *
     * inputColumns would have the value [("Input", "Alpha"), ("Input", "Beta")] to encode that the column arguments
     * to the 2-parameter sum exector should be the two named columns from the Input table.
     *
     * columnsToCopyToOutputTable would have the value [("Input", "Alpha"), ("Input", "Beta")] to encode that
     * Output should contain not just the result column from sum, but also copies of Alpha and Beta from Input.
     *
     * outputTableId would have the value "Output" to encode the name of the created table.
     */
    class ApplyMethod : public ApplyBase
    {

    public:

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
        ApplyMethod(string executorId, string functionId, string outputTableId, string outputColumnId,  TableColumns inputColumns,
                      shared_ptr<vector<Column>> columnsToCopyToOutputTable);

        /**
         * Visitiation hook for visitor patttern.
         */
        void match(function<void(Load&)>, function<void(ApplyFunction&)>,
                   function<void(ApplyMethod&)> forApplyMethod, function<void(Filter&)>,
                   function<void(Hoist&)>, function<void(GreaterThan&)>, function<void(Store&)>) override;
    };

    typedef shared_ptr<ApplyMethod> ApplyMethodPtr;

    ApplyMethodPtr makeApplyMethod(string executorId, string functionId, string outputTableId, string outputColumnId,
                                   TableColumns inputColumns, shared_ptr<vector<Column>> columnsToCopyToOutputTable);
}

#endif //PDB_TCAPINTERMEDIARYREP_APPLYFUNCTION_H
