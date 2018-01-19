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
#ifndef PDB_TCAPINTERMEDIARYREP_GREATERTHAN_H
#define PDB_TCAPINTERMEDIARYREP_GREATERTHAN_H

#include <memory>
#include <vector>

#include "TableColumn.h"
#include "Instruction.h"

using std::shared_ptr;
using std::vector;

namespace pdb_detail {
/**
 * An instruction that describes how to create a new table from existing table(s) by application of
 * an
 * externally defined executor that can compare values using a "greater than" operation.
 *
 * For example, consider an executor "comp" that consumes two integer columns from two tables to
 * produce a
 * new result column.
 *
 * For the input tables "Brother" and "Sister"
 *
 * Brother:                                 Sister:
 * ------------|----------                  -------------|----------
 *    Name     |   Age                         Name      |   Age
 * ------------|----------                  -------------|----------
 *   "James"   |    9                         "Mary"     |    7
 * ------------|----------                  -------------|----------
 *   "Will"    |    3                         "Margaret" |    5
 * ------------|----------                  -------------|----------
 *
 * comp could take the Age coluns from each table and compare them to create "Result" of the form:
 *
 * Result:
 * |----------|----------------
 * |  Name    | hasLittleSister
 * |----------|----------------
 * |  "James" |  true
 * |----------|----------------
 * |  "Will"  |  false
 * |----------|----------------
 *
 *
 * An GreaterThan instruction could the be created to decribe how to take the input tables and
 * produce
 * Result.
 *
 * In this example the GreaterThan instruction that described creation of the Result table above
 * would have the
 * following form:
 *
 * leftHandSide would be ("Brother", "Age") to denote the left hand value for a comparison
 *
 * rightHandSide would be ("Sister", "Age") to denote the right hand value for a comparison
 *
 * outputColumn would have the value ("Result", "hasLittleSister") to denote the name of the table
 * to be created
 * and the name of the column in that table to store the comparison result for each row.
 *
 * columnsToCopyToOutputTable would have the value [("Brother", "Name")] to encode that
 * Result should contain not just the result column from sum, but also a copy of the Name column
 * from the Brother
 * table.
 *
 */
class GreaterThan : public Instruction {
public:
    /**
     * The table/column to use as the right hand side for comparison.
     */
    const TableColumn leftHandSide;

    /**
     * The table/column to use as the right hand side for comparison.
     */
    const TableColumn rightHandSide;

    /**
     * The table/column to store the comparision result.
     */
    const TableColumn outputColumn;

    /**
     * Any optional columns to copy into the output table during its contruction.
     */
    const shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable;

    /**
     * An identifier specifying the executor to be used to create the output column.
     */
    const string executorId;

    // contract from super
    void match(function<void(Load&)> forLoad,
               function<void(ApplyFunction&)>,
               function<void(ApplyMethod&)>,
               function<void(Filter&)>,
               function<void(Hoist&)>,
               function<void(GreaterThan&)> forGreaterThan,
               function<void(Store&)>);

private:
    /**
     * Creates a new GreaterThan instruction.
     *
     * If columnsToCopyToOutputTable is nullptr,  throws invalid_argument.
     *
     * @param leftHandSide The table/column to use as the right hand side for comparison.
     * @param rightHandSide The table/column to use as the right hand side for comparison.
     * @param outputColumn  The table/column to store the comparision result.
     * @param columnsToCopyToOutputTable Any optional columns to copy into the output table during
     * its contruction.
     * @param executorId An identifier specifying the executor to be used to create the output
     * column.
     * @return the new GreaterThan instruction
     */
    GreaterThan(TableColumn leftHandSide,
                TableColumn rightHandSide,
                TableColumn outputColumn,
                shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable,
                const string& executorId);

    // for constructor
    friend shared_ptr<GreaterThan> makeGreaterThan(
        TableColumn leftHandSide,
        TableColumn rightHandSide,
        TableColumn outputColumn,
        shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable,
        const string& executorId);

    // for constructor
    friend InstructionPtr makeInstructionFromAssignment(
        const class TableAssignment& tableAssignment);
};

typedef shared_ptr<GreaterThan> GreaterThanPtr;

/**
 * Creates a new GreaterThan instruction.
 *
 * If columnsToCopyToOutputTable is nullptr, returns nulltpr.
 *
 * @param leftHandSide The table/column to use as the right hand side for comparison.
 * @param rightHandSide The table/column to use as the right hand side for comparison.
 * @param outputColumn  The table/column to store the comparision result.
 * @param columnsToCopyToOutputTable Any optional columns to copy into the output table during its
 * contruction.
 * @param executorId An identifier specifying the executor to be used to create the output column.
 * @return a shared pointer to the new GreaterThan instruction or nullptr
 */
GreaterThanPtr makeGreaterThan(TableColumn leftHandSide,
                               TableColumn rightHandSide,
                               TableColumn outputColumn,
                               shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable,
                               const string& executorId);
}

#endif  // PDB_TCAPINTERMEDIARYREP_GREATERTHAN_H
