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

namespace pdb_detail
{
    string getAttributeValue(string name, shared_ptr<const vector<TcapAttribute>> attributes)
    {
        for(TcapAttribute a : *attributes.get())
        {
            if(a.name.contents == name)
            {
                string s = a.value.contents;
                s.erase(std::remove(s.begin(), s.end(), '"'), s.end());
                return s;
            }
        }

        throw "no such element";
    }

    shared_ptr<Instruction> makeInstructionFromApply(ApplyOperation &applyOp, TableAssignment& tableAssignment)
    {
        string executorId = getAttributeValue("exec", tableAssignment.attributes);

        shared_ptr<vector<Column>> empty = make_shared<vector<Column>>();

        string inputTableName = applyOp.inputTable.contents;

        // make tableColumns
        shared_ptr<vector<string>> columnNames = make_shared<vector<string>>();
        {
            for (TcapIdentifier i : *applyOp.inputTableColumnNames.get())
            {
                columnNames->push_back(i.contents);
            }
        }
        TableColumns inputColumns(applyOp.inputTable.contents, columnNames);

        shared_ptr<vector<Column>> outputColumnsToCopy = make_shared<vector<Column>>();
        {
            for(std::vector<int>::size_type i = 0; i < tableAssignment.columnNames->size()-1; i++)
            {
                string columnId = tableAssignment.columnNames->operator[](i).contents;
                outputColumnsToCopy->push_back(Column(inputTableName, columnId));
            }
        }

        string functionId = applyOp.applyTarget;
        string outputTableName = tableAssignment.tableName.contents;

        // outputcolumnName is the last value in tableAssignment.columnNames
        string outputcolumnName = tableAssignment.columnNames->operator[](tableAssignment.columnNames->size()-1).contents;

        if(applyOp.applyType == ApplyOperationType::func)
            return make_shared<ApplyFunction>(executorId, functionId , outputTableName, outputcolumnName, inputColumns, outputColumnsToCopy);
        else
            return make_shared<ApplyMethod>(executorId, functionId , outputTableName, outputcolumnName, inputColumns, outputColumnsToCopy);

    }

    shared_ptr<Instruction> makeInstruction(TableAssignment& tableAssignment)
    {
        shared_ptr<Instruction> instruction;

        string assignmentLhsTableName = tableAssignment.tableName.contents;

        tableAssignment.value->match(
                [&](LoadOperation &load)
                {
                    string unquotedSource = load.source.substr(1, load.source.size() - 2); // remove surrounding quotes
                    instruction = make_shared<Load>(assignmentLhsTableName,
                                                    tableAssignment.columnNames->operator[](0).contents,
                                                    unquotedSource);
                },
                [&](ApplyOperation &applyOp)
                {
                    instruction = makeInstructionFromApply(applyOp, tableAssignment);

                },
                [&](FilterOperation &filterOperation)
                {
                    string inputTableName = filterOperation.inputTableName.contents;
                    shared_ptr<vector<Column>> outputColumnsToCopy = make_shared<vector<Column>>();
                    {
                        for (std::vector<int>::size_type i = 0; i < tableAssignment.columnNames->size(); i++)
                        {
                            string columnId = tableAssignment.columnNames->operator[](i).contents;
                            outputColumnsToCopy->push_back(Column(inputTableName, columnId));
                        }
                    }

                    instruction = make_shared<Filter>(inputTableName, filterOperation.filterColumnName.contents,
                                                      assignmentLhsTableName, outputColumnsToCopy);

                },
                [&](HoistOperation &hoistOperation)
                {
                    string inputTableName = hoistOperation.inputTable.contents;
                    shared_ptr<vector<Column>> outputColumnsToCopy = make_shared<vector<Column>>();
                    {
                        for (std::vector<int>::size_type i = 0; i < tableAssignment.columnNames->size() - 1; i++)
                        {
                            string columnId = tableAssignment.columnNames->operator[](i).contents;
                            outputColumnsToCopy->push_back(Column(inputTableName, columnId));
                        }
                    }

                    Column hoistColumn(hoistOperation.inputTable.contents,
                                       hoistOperation.inputTableColumnName.contents);

                    string executorId = getAttributeValue("exec", tableAssignment.attributes);

                    string outputcolumnName = tableAssignment.columnNames->operator[](
                            tableAssignment.columnNames->size() - 1).contents;

                    Column outputColumn(assignmentLhsTableName, outputcolumnName);

                    instruction = make_shared<Hoist>(hoistOperation.hoistTarget, hoistColumn,
                                                     outputColumn, outputColumnsToCopy,
                                                     executorId);

                },
                [&](BinaryOperation &binaryOperation)
                {
                    binaryOperation.execute(
                            [&](GreaterThanOp &gt)
                            {
                                Column lhs(gt.lhsTableName.contents, gt.lhsColumnName.contents);
                                Column rhs(gt.rhsTableName.contents, gt.rhsColumnName.contents);

                                string inputTableName = lhs.tableId;
                                shared_ptr<vector<Column>> outputColumnsToCopy = make_shared<vector<Column>>();
                                {
                                    for (std::vector<int>::size_type i = 0;
                                         i < tableAssignment.columnNames->size() - 1; i++)
                                    {
                                        string columnId = tableAssignment.columnNames->operator[](i).contents;
                                        outputColumnsToCopy->push_back(Column(inputTableName, columnId));
                                    }
                                }

                                string executorId = getAttributeValue("exec", tableAssignment.attributes);

                                string outputcolumnName = tableAssignment.columnNames->operator[](
                                        tableAssignment.columnNames->size() - 1).contents;

                                Column outputColumn(assignmentLhsTableName, outputcolumnName);

                                instruction = make_shared<GreaterThan>(lhs, rhs, outputColumn, outputColumnsToCopy,
                                                                       executorId);
                            });
                });

        return instruction;
    }

    shared_ptr<Instruction> makeInstruction(StoreOperation& storeOperation)
    {
        string unquotedDest = storeOperation.destination;
        unquotedDest = unquotedDest.substr(1, unquotedDest.size()-2); // remove surrounding quotes

        shared_ptr<vector<string>> columnsToStore = make_shared<vector<string>>();
        {
            for(TcapIdentifier c : *storeOperation.columnsToStore.get())
            {
                columnsToStore->push_back(c.contents);
            }
        }

        return make_shared<Store>(TableColumns(storeOperation.outputTable.contents, columnsToStore), unquotedDest);
    }

    shared_ptr<Instruction> makeInstruction(shared_ptr<TcapStatement> stmt)
    {
        shared_ptr<Instruction> translated = nullptr;
        stmt->match([&](TableAssignment &tableAssignment)
                    {
                        translated = makeInstruction(tableAssignment);
                    },
                    [&](StoreOperation &storeOperation)
                    {
                        translated = makeInstruction(storeOperation);
                    });

        return translated;
    }


    shared_ptr<vector<shared_ptr<Instruction>>> buildTcapIr(TranslationUnit unit)
    {
        shared_ptr<vector<shared_ptr<Instruction>>> instructions = make_shared<vector<shared_ptr<Instruction>>>();

        for(shared_ptr<TcapStatement> stmt : *unit.statements.get())
        {
            try
            {
                instructions->push_back(makeInstruction(stmt));
            }
            catch (...)
            {
                return nullptr;
            }
        }

        return instructions;
    }
}