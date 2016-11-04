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
//
// Created by barnett on 10/25/16.
//

#include "BuildTcapIrTests.h"

#include "ApplyMethod.h"
#include "Store.h"
#include "TcapParser.h"
#include "TcapIrBuilder.h"

using pdb_detail::buildTcapIr;
using pdb_detail::TranslationUnit;
using pdb_detail::ApplyOperation;
using pdb_detail::ApplyOperationType;
using pdb_detail::FilterOperation;
using pdb_detail::RetainAllClause;
using pdb_detail::RetainExplicitClause;
using pdb_detail::LoadOperation;
using pdb_detail::TableAssignment;
using pdb_detail::Identifier;
using pdb_detail::GreaterThan;
using pdb_detail::GreaterThanOp;
using pdb_detail::TableExpression;
using pdb_detail::HoistOperation;
using pdb_detail::Instruction;
using pdb_detail::InstructionType;
using pdb_detail::ApplyFunction;
using pdb_detail::ApplyMethod;
using pdb_detail::Attribute;
using pdb_detail::StoreOperation;
using pdb_detail::Store;

namespace pdb_tests
{
    void buildTcapIrTest1(UnitTest &qunit)
    {
        shared_ptr<TableAssignment> loadAssignment;
        {
            shared_ptr<vector<Identifier>> cols = make_shared<vector<Identifier>>();
            cols->push_back(Identifier("one"));

            shared_ptr<TableExpression> load = make_shared<LoadOperation>("\"source\"");

            loadAssignment = make_shared<TableAssignment>(Identifier("A"), cols, load);
        }

        shared_ptr<TranslationUnit> unit = make_shared<TranslationUnit>();
        {
            unit->statements->push_back(loadAssignment);
        }

        shared_ptr<vector<shared_ptr<Instruction>>> instructions = buildTcapIr(unit);

        QUNIT_IS_EQUAL(1, instructions->size());

        instructions->operator[](0)->match(
                [&](Load& load)
                {
                    QUNIT_IS_EQUAL("source", load.source);
                    QUNIT_IS_EQUAL("A", load.outputTableId);
                    QUNIT_IS_EQUAL(InstructionType::load, load.instructionType);
                },
                [&](ApplyFunction&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyMethod&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Filter&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Hoist&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](GreaterThan&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Store&)
                {
                    QUNIT_IS_TRUE(false);
                });
    }

    void buildTcapIrTest2(UnitTest &qunit)
    {
        shared_ptr<TableAssignment> applyAssignment;
        {
            shared_ptr<vector<Identifier>> outputTableCols = make_shared<vector<Identifier>>();
            outputTableCols->push_back(Identifier("student"));
            outputTableCols->push_back(Identifier("examAverage"));

            shared_ptr<vector<Identifier>> inputTableCols = make_shared<vector<Identifier>>();
            inputTableCols->push_back(Identifier("student"));

            shared_ptr<TableExpression> apply = make_shared<ApplyOperation>("averageExams", ApplyOperationType::func, Identifier("A"), inputTableCols, make_shared<RetainAllClause> ());

            applyAssignment = make_shared<TableAssignment>(Identifier("B"), outputTableCols, apply);
            applyAssignment->attributes->push_back(Attribute(Identifier("exec"), make_shared<string>("\"exec1\"")));
        }

        shared_ptr<TranslationUnit> unit = make_shared<TranslationUnit>();
        {
            unit->statements->push_back(applyAssignment);
        }

        shared_ptr<vector<shared_ptr<Instruction>>> instructions = buildTcapIr(unit);

        QUNIT_IS_EQUAL(1, instructions->size());

        instructions->operator[](0)->match(
                [&](Load&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyFunction& applyFunction)
                {
                    QUNIT_IS_EQUAL("exec1", applyFunction.executorId);
                    QUNIT_IS_EQUAL("averageExams", applyFunction.functionId);
                    QUNIT_IS_EQUAL("examAverage", applyFunction.outputColumnId);


                    QUNIT_IS_EQUAL(1, applyFunction.inputColumns.columnIds->size());
                    QUNIT_IS_EQUAL("A", applyFunction.inputColumns.tableName);
                    QUNIT_IS_EQUAL("student", applyFunction.inputColumns.operator[](0));

                    QUNIT_IS_EQUAL("B", applyFunction.outputTableId);

                    QUNIT_IS_EQUAL(1, applyFunction.columnsToCopyToOutputTable->size());
                    QUNIT_IS_EQUAL("A", applyFunction.columnsToCopyToOutputTable->operator[](0).tableId);
                    QUNIT_IS_EQUAL("student", applyFunction.columnsToCopyToOutputTable->operator[](0).columnId);
                },
                [&](ApplyMethod&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Filter&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Hoist&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](GreaterThan&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Store&)
                {
                    QUNIT_IS_TRUE(false);
                });
    }

    void buildTcapIrTest3(UnitTest &qunit)
    {
        shared_ptr<TableAssignment> applyAssignment;
        {
            shared_ptr<vector<Identifier>> outputTableCols = make_shared<vector<Identifier>>();
            outputTableCols->push_back(Identifier("student"));
            outputTableCols->push_back(Identifier("examAverage"));

            shared_ptr<vector<Identifier>> inputTableCols = make_shared<vector<Identifier>>();
            inputTableCols->push_back(Identifier("student"));

            shared_ptr<TableExpression> apply = make_shared<ApplyOperation>("averageExams", ApplyOperationType::method, Identifier("A"), inputTableCols, make_shared<RetainAllClause> ());

            applyAssignment = make_shared<TableAssignment>(Identifier("B"), outputTableCols, apply);
            applyAssignment->attributes->push_back(Attribute(Identifier("exec"), make_shared<string>("\"exec1\"")));
        }

        shared_ptr<TranslationUnit> unit = make_shared<TranslationUnit>();
        {
            unit->statements->push_back(applyAssignment);
        }

        shared_ptr<vector<shared_ptr<Instruction>>> instructions = buildTcapIr(unit);

        QUNIT_IS_EQUAL(1, instructions->size());

        instructions->operator[](0)->match(
                [&](Load&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyFunction&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyMethod& applyMethod)
                {
                    QUNIT_IS_EQUAL("exec1", applyMethod.executorId);
                    QUNIT_IS_EQUAL("averageExams", applyMethod.functionId);
                    QUNIT_IS_EQUAL("examAverage", applyMethod.outputColumnId);


                    QUNIT_IS_EQUAL(1, applyMethod.inputColumns.columnIds->size());
                    QUNIT_IS_EQUAL("A", applyMethod.inputColumns.tableName);
                    QUNIT_IS_EQUAL("student", applyMethod.inputColumns.operator[](0));

                    QUNIT_IS_EQUAL("B", applyMethod.outputTableId);

                    QUNIT_IS_EQUAL(1, applyMethod.columnsToCopyToOutputTable->size());
                    QUNIT_IS_EQUAL("A", applyMethod.columnsToCopyToOutputTable->operator[](0).tableId);
                    QUNIT_IS_EQUAL("student", applyMethod.columnsToCopyToOutputTable->operator[](0).columnId);
                },
                [&](Filter&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Hoist&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](GreaterThan&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Store&)
                {
                    QUNIT_IS_TRUE(false);
                });
    }

    void buildTcapIrTest4(UnitTest &qunit)
    {
        shared_ptr<TableAssignment> filterAssignment;
        {
            shared_ptr<vector<Identifier>> outputTableCols = make_shared<vector<Identifier>>();
            outputTableCols->push_back(Identifier("student"));

            shared_ptr<vector<Identifier>> inputTableCols = make_shared<vector<Identifier>>();
            inputTableCols->push_back(Identifier("isExamGreater"));

            shared_ptr<TableExpression> filter = make_shared<FilterOperation>(Identifier("D"), Identifier("isExamGreater"),
                                                                              make_shared<RetainExplicitClause> (Identifier("student")));

            filterAssignment = make_shared<TableAssignment>(Identifier("E"), outputTableCols, filter);
            filterAssignment->attributes->push_back(Attribute(Identifier("exec"), make_shared<string>("\"exec4\"")));
        }

        shared_ptr<TranslationUnit> unit = make_shared<TranslationUnit>();
        {
            unit->statements->push_back(filterAssignment);
        }

        shared_ptr<vector<shared_ptr<Instruction>>> instructions = buildTcapIr(unit);

        QUNIT_IS_EQUAL(1, instructions->size());

        instructions->operator[](0)->match(
                [&](Load&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyFunction&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyMethod&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Filter& filter)
                {
                    QUNIT_IS_EQUAL("D", filter.inputTableId);

                    QUNIT_IS_EQUAL("isExamGreater", filter.filterColumnId);

                    QUNIT_IS_EQUAL("E", filter.outputTableId);

                    QUNIT_IS_EQUAL(1, filter.columnsToCopyToOutputTable->size());
                    QUNIT_IS_EQUAL("D", filter.columnsToCopyToOutputTable->operator[](0).tableId);
                    QUNIT_IS_EQUAL("student", filter.columnsToCopyToOutputTable->operator[](0).columnId);
                },
                [&](Hoist&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](GreaterThan&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Store&)
                {
                    QUNIT_IS_TRUE(false);
                });
    }

    void buildTcapIrTest5(UnitTest &qunit)
    {
        shared_ptr<TableAssignment> hoistAssignment;
        {
            shared_ptr<vector<Identifier>> outputTableCols = make_shared<vector<Identifier>>();
            outputTableCols->push_back(Identifier("student"));

            shared_ptr<vector<Identifier>> inputTableCols = make_shared<vector<Identifier>>();
            inputTableCols->push_back(Identifier("student"));

            shared_ptr<TableExpression> hoist = make_shared<HoistOperation>("homeworkAverage", Identifier("B"),
                                                                            inputTableCols, make_shared<RetainAllClause>());

            hoistAssignment = make_shared<TableAssignment>(Identifier("E"), outputTableCols, hoist);
            hoistAssignment->attributes->push_back(Attribute(Identifier("exec"), make_shared<string>("\"exec5\"")));
        }

        shared_ptr<TranslationUnit> unit = make_shared<TranslationUnit>();
        {
            unit->statements->push_back(hoistAssignment);
        }

        shared_ptr<vector<shared_ptr<Instruction>>> instructions = buildTcapIr(unit);

        QUNIT_IS_EQUAL(1, instructions->size());

        instructions->operator[](0)->match(
                [&](Load&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyFunction&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyMethod&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Filter& )
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Hoist& hoist)
                {
                    QUNIT_IS_EQUAL("exec5", hoist.executorId);

                    QUNIT_IS_EQUAL("E", hoist.outputColumn.tableId);

                    QUNIT_IS_EQUAL("homeworkAverage", hoist.fieldId);

                    QUNIT_IS_EQUAL("B", hoist.inputColumn.tableId);

                    QUNIT_IS_EQUAL("student", hoist.inputColumn.columnId);

                    QUNIT_IS_EQUAL(0, hoist.columnsToCopyToOutputTable->size());
                },
                [&](GreaterThan&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Store&)
                {
                    QUNIT_IS_TRUE(false);
                });
    }

    void buildTcapIrTest6(UnitTest &qunit)
    {
        shared_ptr<TableAssignment> greaterThanAssignment;
        {
            shared_ptr<vector<Identifier>> outputTableCols = make_shared<vector<Identifier>>();
            outputTableCols->push_back(Identifier("student"));
            outputTableCols->push_back(Identifier("isExamGreater"));


            shared_ptr<TableExpression> gt = make_shared<GreaterThanOp>(Identifier("C"), Identifier("examAverage"),
                                                                        Identifier("C"), Identifier("hwAverage"),
                                                                        make_shared<RetainExplicitClause> (Identifier("student")));

            greaterThanAssignment = make_shared<TableAssignment>(Identifier("E"), outputTableCols, gt);
            greaterThanAssignment->attributes->push_back(Attribute(Identifier("exec"), make_shared<string>("\"exec6\"")));
        }

        shared_ptr<TranslationUnit> unit = make_shared<TranslationUnit>();
        {
            unit->statements->push_back(greaterThanAssignment);
        }

        shared_ptr<vector<shared_ptr<Instruction>>> instructions = buildTcapIr(unit);

        QUNIT_IS_EQUAL(1, instructions->size());

        instructions->operator[](0)->match(
                [&](Load&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyFunction&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyMethod&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Filter& )
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Hoist&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](GreaterThan& greaterThan)
                {
                    QUNIT_IS_EQUAL("exec6", greaterThan.executorId);
                    QUNIT_IS_EQUAL("E", greaterThan.outputColumn.tableId);
                    QUNIT_IS_EQUAL("isExamGreater", greaterThan.outputColumn.columnId);


                    QUNIT_IS_EQUAL("C", greaterThan.leftHandSide.tableId);
                    QUNIT_IS_EQUAL("examAverage", greaterThan.leftHandSide.columnId);

                    QUNIT_IS_EQUAL("C", greaterThan.rightHandSide.tableId);
                    QUNIT_IS_EQUAL("hwAverage", greaterThan.rightHandSide.columnId);

                    QUNIT_IS_EQUAL(1, greaterThan.columnsToCopyToOutputTable->size());
                    QUNIT_IS_EQUAL("student", greaterThan.columnsToCopyToOutputTable->operator[](0).columnId);

                },
                [&](Store&)
                {
                    QUNIT_IS_TRUE(false);
                });
    }

    void buildTcapIrTest7(UnitTest &qunit)
    {

        shared_ptr<TranslationUnit> unit = make_shared<TranslationUnit>();
        {
            unit->statements->push_back(make_shared<StoreOperation>(Identifier("A"), "\"desc\""));
        }

        shared_ptr<vector<shared_ptr<Instruction>>> instructions = buildTcapIr(unit);

        QUNIT_IS_EQUAL(1, instructions->size());

        instructions->operator[](0)->match(
                [&](Load& load)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyFunction&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](ApplyMethod&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Filter&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Hoist&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](GreaterThan&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](Store& store)
                {
                    QUNIT_IS_EQUAL("desc", store.destination);
                    QUNIT_IS_EQUAL("A", store.tableId);
                    QUNIT_IS_EQUAL(InstructionType::store, store.instructionType);
                });
    }
}
