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

#include "BuildTcapTests.h"

#include "ApplyOperation.h"
#include "ApplyMethod.h"
#include "FilterOperation.h"
#include "GreaterThanOp.h"
#include "HoistOperation.h"
#include "LoadOperation.h"
#include "RetainExplicitClause.h"
#include "RetainAllClause.h"
#include "Store.h"
#include "TableAssignment.h"
#include "StoreOperation.h"
#include "TcapParser.h"
#include "TcapIdentifier.h"
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
using pdb_detail::TcapIdentifier;
using pdb_detail::TcapStatementPtr;
using pdb_detail::GreaterThan;
using pdb_detail::GreaterThanOp;
using pdb_detail::TableExpression;
using pdb_detail::HoistOperation;
using pdb_detail::Instruction;
using pdb_detail::InstructionType;
using pdb_detail::ApplyFunction;
using pdb_detail::ApplyMethod;
using pdb_detail::TcapAttribute;
using pdb_detail::StoreOperation;
using pdb_detail::Store;
using pdb_detail::StringLiteral;

namespace pdb_tests
{
    void buildTcapIrTest1(UnitTest &qunit)
    {
        shared_ptr<TableAssignment> loadAssignment;
        {
            shared_ptr<vector<TcapIdentifier>> cols = make_shared<vector<TcapIdentifier>>();
            cols->push_back(TcapIdentifier("one"));

            shared_ptr<TableExpression> load = make_shared<LoadOperation>("\"source\"");

            loadAssignment = make_shared<TableAssignment>(TcapIdentifier("A"), cols, load);
        }

        TranslationUnit unit(loadAssignment);

        shared_ptr<vector<shared_ptr<Instruction>>> instructions = buildTcapIr(unit);

        QUNIT_IS_EQUAL(1, instructions->size());

        instructions->operator[](0)->match(
                [&](Load& load)
                {
                    QUNIT_IS_EQUAL("source", load.source);
                    QUNIT_IS_EQUAL("A", load.outputColumn.tableId);
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
            shared_ptr<vector<TcapIdentifier>> outputTableCols = make_shared<vector<TcapIdentifier>>();
            outputTableCols->push_back(TcapIdentifier("student"));
            outputTableCols->push_back(TcapIdentifier("examAverage"));

            shared_ptr<vector<TcapIdentifier>> inputTableCols = make_shared<vector<TcapIdentifier>>();
            inputTableCols->push_back(TcapIdentifier("student"));

            shared_ptr<TableExpression> apply = shared_ptr<ApplyOperation>(
                    new ApplyOperation("averageExams", ApplyOperationType::func, TcapIdentifier("A"), inputTableCols, make_shared<RetainAllClause> ()));

            applyAssignment = make_shared<TableAssignment>(TcapAttribute(TcapIdentifier("exec"), StringLiteral("\"exec1\"")),
                                                           TcapIdentifier("B"), outputTableCols, apply);
        }

        TranslationUnit unit(applyAssignment);


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
            shared_ptr<vector<TcapIdentifier>> outputTableCols = make_shared<vector<TcapIdentifier>>();
            outputTableCols->push_back(TcapIdentifier("student"));
            outputTableCols->push_back(TcapIdentifier("examAverage"));

            shared_ptr<vector<TcapIdentifier>> inputTableCols = make_shared<vector<TcapIdentifier>>();
            inputTableCols->push_back(TcapIdentifier("student"));

            shared_ptr<TableExpression> apply = shared_ptr<ApplyOperation>(
                    new ApplyOperation("averageExams", ApplyOperationType::method, TcapIdentifier("A"), inputTableCols, make_shared<RetainAllClause> ()));

            applyAssignment = make_shared<TableAssignment>(TcapAttribute(TcapIdentifier("exec"), StringLiteral("\"exec1\"")),
                                                           TcapIdentifier("B"), outputTableCols, apply);
        }

        TranslationUnit unit = TranslationUnit(applyAssignment);

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
            shared_ptr<vector<TcapIdentifier>> outputTableCols = make_shared<vector<TcapIdentifier>>();
            outputTableCols->push_back(TcapIdentifier("student"));

            shared_ptr<vector<TcapIdentifier>> inputTableCols = make_shared<vector<TcapIdentifier>>();
            inputTableCols->push_back(TcapIdentifier("isExamGreater"));

            shared_ptr<TableExpression> filter = shared_ptr<FilterOperation>(
                    new FilterOperation(TcapIdentifier("D"), TcapIdentifier("isExamGreater"),
                                        make_shared<RetainExplicitClause> (TcapIdentifier("student"))));

            filterAssignment = make_shared<TableAssignment>(TcapAttribute(TcapIdentifier("exec"), StringLiteral("\"exec4\"")),
                                                            TcapIdentifier("E"), outputTableCols, filter);
        }

        TranslationUnit unit = TranslationUnit(filterAssignment);


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
            shared_ptr<vector<TcapIdentifier>> outputTableCols = make_shared<vector<TcapIdentifier>>();
            outputTableCols->push_back(TcapIdentifier("student"));

            TcapIdentifier student("student");

            shared_ptr<TableExpression> hoist( new HoistOperation("homeworkAverage", TcapIdentifier("B"),
                                                                  student, make_shared<RetainAllClause>()));

            TcapAttribute attribute(TcapIdentifier("exec"), StringLiteral("\"exec5\""));
            hoistAssignment = make_shared<TableAssignment>(attribute, TcapIdentifier("E"), outputTableCols, hoist);
        }

        TranslationUnit unit = TranslationUnit(hoistAssignment);

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
            shared_ptr<vector<TcapIdentifier>> outputTableCols = make_shared<vector<TcapIdentifier>>();
            outputTableCols->push_back(TcapIdentifier("student"));
            outputTableCols->push_back(TcapIdentifier("isExamGreater"));


            shared_ptr<GreaterThanOp> gt( new GreaterThanOp(TcapIdentifier("C"), TcapIdentifier("examAverage"),
                                                                        TcapIdentifier("C"), TcapIdentifier("hwAverage"),
                                                                        make_shared<RetainExplicitClause> (TcapIdentifier("student"))));

            greaterThanAssignment = make_shared<TableAssignment>(TcapAttribute(TcapIdentifier("exec"), StringLiteral("\"exec6\"")),
                                                                 TcapIdentifier("E"), outputTableCols, gt);
        }

        TranslationUnit unit = TranslationUnit(greaterThanAssignment);


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
        shared_ptr<vector<TcapIdentifier>> columnsToStore = make_shared<vector<TcapIdentifier>>();
        columnsToStore->push_back(TcapIdentifier("1"));
        columnsToStore->push_back(TcapIdentifier("2"));

        TranslationUnit unit(shared_ptr<StoreOperation>(new StoreOperation(TcapIdentifier("A"), columnsToStore, "\"desc\"")));

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
                    QUNIT_IS_EQUAL("A", store.columnsToStore.tableName);
                    QUNIT_IS_EQUAL(2, store.columnsToStore.columnIds->size());
                    QUNIT_IS_EQUAL("1", store.columnsToStore.columnIds->operator[](0));
                    QUNIT_IS_EQUAL("2", store.columnsToStore.columnIds->operator[](1));
                    QUNIT_IS_EQUAL(InstructionType::store, store.instructionType);
                });
    }
}
