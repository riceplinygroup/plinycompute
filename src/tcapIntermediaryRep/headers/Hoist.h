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
#ifndef PDB_TCAPINTERMEDIARYREP_HOIST_H
#define PDB_TCAPINTERMEDIARYREP_HOIST_H

#include <memory>
#include <string>
#include <vector>

#include "TableColumn.h"
#include "Instruction.h"

using std::shared_ptr;
using std::string;
using std::vector;

using pdb_detail::TableColumn;
using pdb_detail::Instruction;

namespace pdb_detail {

/**
 * An instruction that describes how to create a new table from an existing table by extracting a
 * field value
 * from each row of an input table to the output table.
 *
 * For example, consider an executor "comp" that extracts a field value from cell values in a table.
 *
 * For the input tables "Persons":
 *
 * --------------------------|
 *    Person                 |
 * --------------------------|
 *   {name:"James", age:34 } |
 * --------------------------|
 *   {name:"Will", age:44 }  |
 * --------------------------|
 *
 * comp could take the Person column an extract the age value to form a new column in a table Result
 *
 * --------------------------|--------------
 *    Person                 |     Age
 * --------------------------|--------------
 *   {name:"James", age:34 } |     34
 * --------------------------|--------------
 *   {name:"Will", age:44 }  |     44
 * --------------------------|--------------
 *
 *
 * A Hoist instruction could the be created to decribe how to take the input Persons and produce
 * Result using comp.
 *
 * In this example the Hoist instruction that described creation of the Result table above would
 * have the
 * following form:
 *
 * inputColumn would be ("Persons", "Person") to denote the source column
 *
 * outputColumn would be ("Result", "Age") to denote the storage location of the hoisted value
 *
 * outputColumn would have the value ("Result", "hasLittleSister") to denote the name of the table
 * to be created
 * and the name of the column in that table to store the comparison result for each row.
 *
 * executorId would be "comp" to encode that the comp executor should be used to create the
 * output collum.
 *
 * columnsToCopyToOutputTable would have the value [("Persons", "Person")] to encode that
 * Result should contain not just the result column from comp, but also a copy of the Person column
 * from the
 * Persons table.
 *
 * fieldId would be some metadata value to describe the source field being hoisted.
 *
 */
class Hoist : public Instruction {
public:
    /**
     * A metadata field to describe the field being hoisted. Does not influence the Hoist operation.
     */
    const string fieldId;

    /**
     * The table/column source from which a field will be hoisted.
     */
    const TableColumn inputColumn;

    /**
     * The table/column output where the field will be copied.
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

    void match(function<void(Load&)>,
               function<void(ApplyFunction&)>,
               function<void(ApplyMethod&)>,
               function<void(Filter&)>,
               function<void(Hoist&)> forHoist,
               function<void(GreaterThan&)>,
               function<void(Store&)>);

private:
    /**
     * Creates a new hoist operation.
     *
     * If columnsToCopyToOutputTable is nullptr, throws invalid_argument exception.
     *
     * @param fieldId  A metadata field to describe the field being hoisted. Does not influence the
     * Hoist operation.
     * @param inputColumn The table/column source from which a field will be hoisted.
     * @param outputColumn The table/column output where the field will be copied.
     * @param columnsToCopyToOutputTable Any optional columns to copy into the output table during
     * its contruction.
     * @param executorId An identifier specifying the executor to be used to create the output
     * column.
     * @return a new Hoist operation
     */
    Hoist(const string& fieldId,
          TableColumn inputColumn,
          TableColumn outputColumn,
          shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable,
          const string& executorId);

    // for constructor
    friend shared_ptr<Hoist> makeHoist(const string& fieldId,
                                       TableColumn inputColumn,
                                       TableColumn outputColumn,
                                       shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable,
                                       const string& executorId);
    // for constructor
    friend InstructionPtr makeInstructionFromAssignment(
        const class TableAssignment& tableAssignment);
};

typedef shared_ptr<Hoist> HoistPtr;

/**
 * Creates a new hoist operation.
 *
 *  If columnsToCopyToOutputTable is nullptr, returns nullptr.
 *
 * @param fieldId  A metadata field to describe the field being hoisted. Does not influence the
 * Hoist operation.
 * @param inputColumn The table/column source from which a field will be hoisted.
 * @param outputColumn The table/column output where the field will be copied.
 * @param columnsToCopyToOutputTable Any optional columns to copy into the output table during its
 * contruction.
 * @param executorId An identifier specifying the executor to be used to create the output column.
 * @return a shared pointer to the new Hoist operation or nullptr
 */
HoistPtr makeHoist(const string& fieldId,
                   TableColumn inputColumn,
                   TableColumn outputColumn,
                   shared_ptr<vector<TableColumn>> columnsToCopyToOutputTable,
                   const string& executorId);
}

#endif  // PDB_TCAPINTERMEDIARYREP_HOIST_H
