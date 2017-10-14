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
#include "TcapIrBuilder.h"

#include "ApplyOperation.h"
#include "FilterOperation.h"
#include "GreaterThanOp.h"
#include "LoadOperation.h"
#include "HoistOperation.h"
#include "Store.h"
#include "StoreOperation.h"
#include "TableAssignment.h"

namespace pdb_detail {
/**
 * @return the value in attributes with key name, or, throws string exception if no match is found.
 */
string getAttributeValue(const string& name, shared_ptr<const vector<TcapAttribute>> attributes) {
    for (TcapAttribute a : *attributes.get()) {
        if (a.name.contents == name) {
            string s = a.value.contents;
            s.erase(std::remove(s.begin(), s.end(), '"'), s.end());  // remove quotes around value
            return s;
        }
    }

    throw "no such element";
}

/**
 * Translate the ApplyOperation from the given TableAssignment into ApplyFunction or ApplyMethod.
 *
 * @param applyOp the applyOp to translate
 * @param tableAssignment the assigment containing applyOp
 * @return  ApplyFunction or ApplyMethod depending onf the flavor of applyOp
 */
InstructionPtr makeInstructionFromApply(const ApplyOperation& applyOp,
                                        const TableAssignment& tableAssignment) {

    string executorId = getAttributeValue("exec", tableAssignment.attributes);
    string inputTableName = applyOp.inputTable.contents;
    string functionId = applyOp.applyTarget;
    string outputTableName = tableAssignment.tableName.contents;

    /*
     * Make the input columns to the apply instruction.
     */
    shared_ptr<vector<string>> columnNames = make_shared<vector<string>>();
    {
        for (TcapIdentifier i : *applyOp.inputTableColumnNames.get()) {
            columnNames->push_back(i.contents);
        }
    }
    TableColumns inputColumns(applyOp.inputTable.contents, columnNames);

    /**
     * Make the result output column name.
     */
    string outputcolumnName =
        tableAssignment.columnNames->operator[](tableAssignment.columnNames->size() - 1).contents;

    /*
     * Make the columns to copy from the input table to the output table.
     */
    shared_ptr<vector<TableColumn>> outputColumnsToCopy = make_shared<vector<TableColumn>>();
    {
        for (std::vector<int>::size_type i = 0; i < tableAssignment.columnNames->size() - 1; i++) {
            string columnId = tableAssignment.columnNames->operator[](i).contents;
            outputColumnsToCopy->push_back(TableColumn(inputTableName, columnId));
        }
    }

    if (applyOp.applyType == ApplyOperationType::func)
        return ApplyFunctionPtr(new ApplyFunction(executorId,
                                                  functionId,
                                                  outputTableName,
                                                  outputcolumnName,
                                                  inputColumns,
                                                  outputColumnsToCopy));
    else  // must be method
        return ApplyMethodPtr(new ApplyMethod(executorId,
                                              functionId,
                                              outputTableName,
                                              outputcolumnName,
                                              inputColumns,
                                              outputColumnsToCopy));
}

/**
 * Translate the given TableAssignment to an Instruction.
 *
 * @param tableAssignment the instruction to transalte
 * @return the translated instruction
 */
InstructionPtr makeInstructionFromAssignment(const TableAssignment& tableAssignment) {
    InstructionPtr instruction;  // the translation of tableAssignment. set by visitor below.

    string assignmentLhsTableName =
        tableAssignment.tableName.contents;  // the table name being assigned to

    tableAssignment.value->match(  // match the rhs of the assignment to one of the expression types

        [&](const LoadOperation& load) {
            string unquotedSource =
                load.source.substr(1, load.source.size() - 2);  // remove surrounding quotes
            instruction = makeLoad(assignmentLhsTableName,
                                   tableAssignment.columnNames->operator[](0).contents,
                                   unquotedSource);
        },
        [&](const ApplyOperation& applyOp) {
            instruction = makeInstructionFromApply(applyOp, tableAssignment);

        },
        [&](const FilterOperation& filterOperation) {
            string inputTableName = filterOperation.inputTableName.contents;
            shared_ptr<vector<TableColumn>> outputColumnsToCopy =
                make_shared<vector<TableColumn>>();
            {
                for (std::vector<int>::size_type i = 0; i < tableAssignment.columnNames->size();
                     i++) {
                    string columnId = tableAssignment.columnNames->operator[](i).contents;
                    outputColumnsToCopy->push_back(TableColumn(inputTableName, columnId));
                }
            }

            instruction = shared_ptr<Filter>(new Filter(inputTableName,
                                                        filterOperation.filterColumnName.contents,
                                                        assignmentLhsTableName,
                                                        outputColumnsToCopy));

        },
        [&](const HoistOperation& hoistOperation) {
            string inputTableName = hoistOperation.inputTable.contents;

            shared_ptr<vector<TableColumn>> outputColumnsToCopy =
                make_shared<vector<TableColumn>>();
            {
                for (std::vector<int>::size_type i = 0; i < tableAssignment.columnNames->size() - 1;
                     i++) {
                    string columnId = tableAssignment.columnNames->operator[](i).contents;
                    outputColumnsToCopy->push_back(TableColumn(inputTableName, columnId));
                }
            }

            TableColumn hoistColumn(hoistOperation.inputTable.contents,
                                    hoistOperation.inputTableColumnName.contents);

            string executorId = getAttributeValue("exec", tableAssignment.attributes);

            string outputcolumnName =
                tableAssignment.columnNames->operator[](tableAssignment.columnNames->size() - 1)
                    .contents;

            TableColumn outputColumn(assignmentLhsTableName, outputcolumnName);

            instruction = shared_ptr<Hoist>(new Hoist(hoistOperation.hoistTarget,
                                                      hoistColumn,
                                                      outputColumn,
                                                      outputColumnsToCopy,
                                                      executorId));

        },
        [&](BinaryOperation& binaryOperation) {
            binaryOperation.execute([&](GreaterThanOp& gt) {
                TableColumn lhs(gt.lhsTableName.contents, gt.lhsColumnName.contents);
                TableColumn rhs(gt.rhsTableName.contents, gt.rhsColumnName.contents);

                string inputTableName = lhs.tableId;

                shared_ptr<vector<TableColumn>> outputColumnsToCopy =
                    make_shared<vector<TableColumn>>();
                {
                    for (std::vector<int>::size_type i = 0;
                         i < tableAssignment.columnNames->size() - 1;
                         i++) {
                        string columnId = tableAssignment.columnNames->operator[](i).contents;
                        outputColumnsToCopy->push_back(TableColumn(inputTableName, columnId));
                    }
                }

                string executorId = getAttributeValue("exec", tableAssignment.attributes);

                string outputcolumnName =
                    tableAssignment.columnNames->operator[](tableAssignment.columnNames->size() - 1)
                        .contents;

                TableColumn outputColumn(assignmentLhsTableName, outputcolumnName);

                instruction = shared_ptr<GreaterThan>(
                    new GreaterThan(lhs, rhs, outputColumn, outputColumnsToCopy, executorId));
            });
        });

    if (instruction == nullptr)
        throw "visitor did not set instruction";

    return instruction;
}

/**
 * Translate the given StoreOperation to a Store.
 *
 * @param storeOperation the instruction to translate
 * @return the translation of storeOperation
 */
StorePtr makeInstructionFromStore(const StoreOperation& storeOperation) {
    string unquotedDest = storeOperation.destination;
    unquotedDest = unquotedDest.substr(1, unquotedDest.size() - 2);  // remove surrounding quotes

    shared_ptr<vector<string>> columnsToStore = make_shared<vector<string>>();
    {
        for (TcapIdentifier c : *storeOperation.columnsToStore.get()) {
            columnsToStore->push_back(c.contents);
        }
    }

    return make_shared<Store>(TableColumns(storeOperation.outputTable.contents, columnsToStore),
                              unquotedDest);
}

/**
 * Translate the given TcapStatement into an Instruction.
 *
 * @param stmt the statement to transalte
 * @return the translation of stmt
 */
InstructionPtr makeInstruction(shared_ptr<TcapStatement> stmt) {
    InstructionPtr translated = nullptr;

    stmt->match(
        [&](TableAssignment& tableAssignment) {
            translated = makeInstructionFromAssignment(tableAssignment);
        },
        [&](StoreOperation& storeOperation) {
            translated = makeInstructionFromStore(storeOperation);
        });

    if (translated == nullptr)
        throw "visitor did not set translated";

    return translated;
}


// contract from .h
shared_ptr<vector<InstructionPtr>> buildTcapIr(TranslationUnit unit) {
    shared_ptr<vector<shared_ptr<Instruction>>> instructions =
        make_shared<vector<shared_ptr<Instruction>>>();

    for (shared_ptr<TcapStatement> stmt : *unit.statements.get())  // translate each statement
    {
        try {
            instructions->push_back(makeInstruction(stmt));
        } catch (...) {
            return nullptr;  // cant throw exceptions beyond API boundires by PDB style rules. So
                             // return nullptr
        }
    }

    return instructions;
}
}