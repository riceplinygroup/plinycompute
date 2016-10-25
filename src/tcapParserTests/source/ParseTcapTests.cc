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
// Created by barnett on 10/12/16.
//

#ifndef PDB_TCAPPARSERTESTS_PARSETCAPTESTS_H
#define PDB_TCAPPARSERTESTS_PARSETCAPTESTS_H


#include "ParseTcapTests.h"

#include <string>

#include "TcapParser.h"

#include "qunit.h"

using std::string;

using pdb_detail::TranslationUnit;
using pdb_detail::ApplyOperation;
using pdb_detail::ApplyOperationType;
using pdb_detail::FilterOperation;
using pdb_detail::LoadOperation;
using pdb_detail::StoreOperation;
using pdb_detail::HoistOperation;
using pdb_detail::BinaryOperation;
using pdb_detail::GreaterThanOp;
using pdb_detail::MemoStatement;
using pdb_detail::TableAssignment;
using pdb_detail::RetainAllClause;
using pdb_detail::RetainNoneClause;
using pdb_detail::RetainExplicitClause;
using pdb_detail::parseTcap;

using QUnit::UnitTest;

namespace pdb_tests
{
    void testParseTcap1(UnitTest &qunit)
    {
       string program =
               "@exec \"exec1\"\n"
               "A(student) = load \"(databaseName, inputSetName)\"\n"
               "B(student, examAverage) = apply func \"avgExams\" to A[student] retain all\n"
               "C(student, examAverage, hwAverage) = hoist \"homeworkAverage\" from B[student] retain all\n"
               "D(student, isExamGreater) = C[examAverage] > C[hwAverage] retain student\n"
               "E(student) = filter D by isExamGreater retain student\n"
               "F(name) = apply method \"getName\" to E[student] retain none"
               "store F \"(databaseName, outputSetName)\"";

        shared_ptr<TranslationUnit> parseTree = parseTcap(program);

        if(parseTree == nullptr)
        {
            QUNIT_IS_TRUE(false);
            return;
        }

        int statementNumber = 0;

        parseTree->statements->operator[](statementNumber++)->execute(
                [&] (MemoStatement&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&] (TableAssignment& assignment)
                {
                    QUNIT_IS_EQUAL(1, assignment.attributes->size());

                    QUNIT_IS_EQUAL("exec", assignment.attributes->operator[](0).name.contents->c_str());
                    QUNIT_IS_EQUAL("\"exec1\"", assignment.attributes->operator[](0).value->c_str());

                    QUNIT_IS_EQUAL("A", assignment.tableName.contents.get()->c_str())

                    QUNIT_IS_EQUAL(1, assignment.columnNames->size());

                    QUNIT_IS_EQUAL("student", assignment.columnNames->operator[](0).contents.get()->c_str())

                    assignment.value->execute(
                            [&] (LoadOperation& load)
                            {
                                QUNIT_IS_EQUAL("\"(databaseName, inputSetName)\"", load.source->c_str())

                            },
                            [&] (ApplyOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (FilterOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (HoistOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (BinaryOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            }
                    );
                },
                [&](StoreOperation&)
                {
                    QUNIT_IS_TRUE(false);
                });

        parseTree->statements->operator[](statementNumber++)->execute(
                [&] (MemoStatement&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&] (TableAssignment& assignment)
                {
                    QUNIT_IS_EQUAL("B", assignment.tableName.contents.get()->c_str())

                    QUNIT_IS_EQUAL(2, assignment.columnNames->size());

                    QUNIT_IS_EQUAL("student", assignment.columnNames->operator[](0).contents.get()->c_str());
                    QUNIT_IS_EQUAL("examAverage", assignment.columnNames->operator[](1).contents.get()->c_str())

                    assignment.value->execute(
                            [&] (LoadOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (ApplyOperation& applyOperation)
                            {
                                QUNIT_IS_EQUAL(ApplyOperationType::func, applyOperation.applyType);

                                QUNIT_IS_EQUAL("\"avgExams\"", applyOperation.applyTarget->c_str())

                                QUNIT_IS_EQUAL("A", applyOperation.inputTable.contents->c_str())

                                QUNIT_IS_EQUAL(1, applyOperation.inputTableColumnNames->size());

                                QUNIT_IS_EQUAL("student", applyOperation.inputTableColumnNames->operator[](0).contents.get()->c_str());

                                QUNIT_IS_TRUE(applyOperation.retain->isAll());
                            },
                            [&] (FilterOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (HoistOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (BinaryOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            });
                },
                [&](StoreOperation&)
                {
                    QUNIT_IS_TRUE(false);
                });

        parseTree->statements->operator[](statementNumber++)->execute(
                [&] (MemoStatement&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&] (TableAssignment& assignment)
                {
                    QUNIT_IS_EQUAL("C", assignment.tableName.contents.get()->c_str())

                    QUNIT_IS_EQUAL(3, assignment.columnNames->size());

                    QUNIT_IS_EQUAL("student", assignment.columnNames->operator[](0).contents.get()->c_str());
                    QUNIT_IS_EQUAL("examAverage", assignment.columnNames->operator[](1).contents.get()->c_str())
                    QUNIT_IS_EQUAL("hwAverage", assignment.columnNames->operator[](2).contents.get()->c_str())

                    assignment.value->execute(
                            [&] (LoadOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (ApplyOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (FilterOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (HoistOperation& hoistOperation)
                            {
                                QUNIT_IS_EQUAL("\"homeworkAverage\"", hoistOperation.hoistTarget->c_str())

                                QUNIT_IS_EQUAL("B", hoistOperation.inputTable.contents->c_str())

                                QUNIT_IS_EQUAL(1, hoistOperation.inputTableColumnNames->size());

                                QUNIT_IS_EQUAL("student", hoistOperation.inputTableColumnNames->operator[](0).contents.get()->c_str());

                                QUNIT_IS_TRUE(hoistOperation.retain->isAll());
                            },
                            [&] (BinaryOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            }
                    );
                },
                [&](StoreOperation&)
                {
                    QUNIT_IS_TRUE(false);
                });

        parseTree->statements->operator[](statementNumber++)->execute(
                [&] (MemoStatement&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&] (TableAssignment& assignment)
                {
                    QUNIT_IS_EQUAL("D", assignment.tableName.contents.get()->c_str())

                    QUNIT_IS_EQUAL(2, assignment.columnNames->size());

                    QUNIT_IS_EQUAL("student", assignment.columnNames->operator[](0).contents.get()->c_str());
                    QUNIT_IS_EQUAL("isExamGreater", assignment.columnNames->operator[](1).contents.get()->c_str())

                    assignment.value->execute(
                            [&] (LoadOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (ApplyOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (FilterOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (HoistOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (BinaryOperation& binOp)
                            {
                                binOp.execute(
                                        [&](GreaterThanOp gt)
                                        {
                                            QUNIT_IS_EQUAL("C", gt.lhsTableName.contents.get()->c_str());
                                            QUNIT_IS_EQUAL("examAverage", gt.lhsColumnName.contents.get()->c_str());

                                            QUNIT_IS_EQUAL("C", gt.rhsTableName.contents.get()->c_str());
                                            QUNIT_IS_EQUAL("hwAverage", gt.rhsColumnName.contents.get()->c_str());
                                        });
                            }
                    );
                },
                [&](StoreOperation&)
                {
                    QUNIT_IS_TRUE(false);
                });

                parseTree->statements->operator[](statementNumber++)->execute(
                [&] (MemoStatement&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&] (TableAssignment& assignment)
                {
                    QUNIT_IS_EQUAL("E", assignment.tableName.contents.get()->c_str())

                    QUNIT_IS_EQUAL(1, assignment.columnNames->size());

                    QUNIT_IS_EQUAL("student", assignment.columnNames->operator[](0).contents.get()->c_str());

                    assignment.value->execute(
                            [&] (LoadOperation)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (ApplyOperation applyOperation)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (FilterOperation filterOperation)
                            {
                                QUNIT_IS_EQUAL("D", filterOperation.inputTableName.contents->c_str())
                                QUNIT_IS_EQUAL("isExamGreater", filterOperation.filterColumnName.contents->c_str())

                                filterOperation.retain->execute(
                                        [&](RetainAllClause)
                                        {
                                            QUNIT_IS_TRUE(false);
                                        },
                                        [&](RetainExplicitClause exp)
                                        {
                                            QUNIT_IS_EQUAL(1, exp.columns->size());
                                            QUNIT_IS_EQUAL("student", exp.columns->operator[](0).contents->c_str())
                                        },
                                        [&](RetainNoneClause)
                                        {
                                            QUNIT_IS_TRUE(false);
                                        });
                            },
                            [&] (HoistOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (BinaryOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            }
                    );
                },
                [&](StoreOperation&)
                {
                    QUNIT_IS_TRUE(false);
                });

        parseTree->statements->operator[](statementNumber++)->execute(
                [&] (MemoStatement&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&] (TableAssignment& assignment)
                {
                    QUNIT_IS_EQUAL("F", assignment.tableName.contents.get()->c_str())

                    QUNIT_IS_EQUAL(1, assignment.columnNames->size());

                    QUNIT_IS_EQUAL("name", assignment.columnNames->operator[](0).contents.get()->c_str());

                    assignment.value->execute(
                            [&] (LoadOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (ApplyOperation& applyOperation)
                            {
                                QUNIT_IS_EQUAL(ApplyOperationType::method, applyOperation.applyType);

                                QUNIT_IS_EQUAL("\"getName\"", applyOperation.applyTarget->c_str())

                                QUNIT_IS_EQUAL("E", applyOperation.inputTable.contents->c_str())

                                QUNIT_IS_EQUAL(1, applyOperation.inputTableColumnNames->size());

                                QUNIT_IS_EQUAL("student", applyOperation.inputTableColumnNames->operator[](0).contents.get()->c_str());

                                QUNIT_IS_TRUE(applyOperation.retain->isNone());
                            },
                            [&] (FilterOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (HoistOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            },
                            [&] (BinaryOperation&)
                            {
                                QUNIT_IS_TRUE(false);
                            });
                },
                [&](StoreOperation&)
                {
                    QUNIT_IS_TRUE(false);
                });

        parseTree->statements->operator[](statementNumber++)->execute(
                [&] (MemoStatement&)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&] (TableAssignment& assignment)
                {
                    QUNIT_IS_TRUE(false);
                },
                [&](StoreOperation& store)
                {
                    QUNIT_IS_EQUAL("F", store.outputTable.contents->c_str());
                    QUNIT_IS_EQUAL("\"(databaseName, outputSetName)\"", store.destination->c_str())
                });
    }
}

#endif //PDB_TCAPPARSERTESTS_PARSETCAPTESTS_H
