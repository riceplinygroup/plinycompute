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

#include "BuildLogicalPlanTests.h"

#include <string>

#include "ApplyFunction.h"
#include "ApplyMethod.h"
#include "Filter.h"
#include "Load.h"
#include "GreaterThan.h"
#include "Hoist.h"
#include "LogicalPlanBuilder.h"
#include "Store.h"
#include "TableColumns.h"

#include "Lexer.h"
#include "Parser.h"
#include "SafeResult.h"

using std::dynamic_pointer_cast;
using std::string;

using pdb_detail::ApplyFunction;
using pdb_detail::ApplyFunctionPtr;
using pdb_detail::ApplyMethod;
using pdb_detail::ApplyMethodPtr;
using pdb_detail::Filter;
using pdb_detail::FilterPtr;
using pdb_detail::GreaterThan;
using pdb_detail::GreaterThanPtr;
using pdb_detail::Hoist;
using pdb_detail::HoistPtr;
using pdb_detail::InstructionPtr;
using pdb_detail::Load;
using pdb_detail::makeApplyMethod;
using pdb_detail::makeFilter;
using pdb_detail::makeGreaterThan;
using pdb_detail::makeHoist;
using pdb_detail::makeLoad;
using pdb_detail::makeStore;
using pdb_detail::Store;
using pdb_detail::TableColumns;

namespace pdb_tests
{
    void testBuildLogicalPlanFromLoad(UnitTest &qunit)
    {
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();

        ops->push_back(makeLoad("table", "column", "1 2"));

        buildLogicalPlan(ops)->apply(
                [&](LogicalPlan logicalPlan)
                {
                    QUNIT_IS_EQUAL(1, logicalPlan.getInputs().size());

                    Input input = logicalPlan.getInputs().getInput("table");
                    QUNIT_IS_EQUAL("table", input.getOutputName());
                    QUNIT_IS_EQUAL("table", input.getOutput().getSetName());
                    QUNIT_IS_EQUAL(1, input.getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("column", input.getOutput().getAtts()[0]);
                    QUNIT_IS_EQUAL("1", input.getDbName());
                    QUNIT_IS_EQUAL("2", input.getSetName());
                },
                [&](string errorMsg)
                {
                    QUNIT_IS_TRUE(false);
                });
    }

    void testBuildLogicalPlanFromApplyFunction(UnitTest &qunit)
    {
        // the instruction to translate
        ApplyFunctionPtr applyFunction;
        {
            shared_ptr<vector<TableColumn>> copyCols = make_shared<vector<TableColumn>>();
            {
                copyCols->push_back(TableColumn("A", "3"));
                copyCols->push_back(TableColumn("A", "4"));
            }

            TableColumns inputCols("A", "1", "2");

            applyFunction = makeApplyFunction("exec", "someFunctionName", "outputTable", "outputColumn", inputCols, copyCols);
        }

        // translate
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();
        ops->push_back(applyFunction);

        buildLogicalPlan(ops)->apply(
                [&](LogicalPlan logicalPlan)
                {
                    // check translation
                    ComputationPtr computation = logicalPlan.getComputations().getProducingComputation("outputTable");

                    QUNIT_IS_EQUAL("Apply", computation->getComputationName());
                    shared_ptr<ApplyLambda> apply = dynamic_pointer_cast<ApplyLambda> (computation);

                    QUNIT_IS_EQUAL("exec", apply->getLambdaToApply());

                    QUNIT_IS_EQUAL("A", apply->getInputName());
                    QUNIT_IS_EQUAL("outputTable", apply->getOutputName());

                    QUNIT_IS_EQUAL(3, apply->getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("3", apply->getOutput().getAtts()[0]);
                    QUNIT_IS_EQUAL("4", apply->getOutput().getAtts()[1]);
                    QUNIT_IS_EQUAL("outputColumn", apply->getOutput().getAtts()[2]);

                    QUNIT_IS_EQUAL(2, apply->getInput().getAtts().size());
                    QUNIT_IS_EQUAL("1", apply->getInput().getAtts()[0]);
                    QUNIT_IS_EQUAL("2", apply->getInput().getAtts()[1]);

                    QUNIT_IS_EQUAL(2, apply->getProjection().getAtts().size());
                    QUNIT_IS_EQUAL("3", apply->getProjection().getAtts()[0]);
                    QUNIT_IS_EQUAL("4", apply->getProjection().getAtts()[1]);
                },
                [&](string errorMsg)
                {
                    QUNIT_IS_TRUE(false);
                });
    }

    void testBuildLogicalPlanFromApplyMethod(UnitTest &qunit)
    {
        // the instruction to translate
        ApplyMethodPtr applyMethod;
        {
            shared_ptr<vector<TableColumn>> copyCols = make_shared<vector<TableColumn>>();
            {
                copyCols->push_back(TableColumn("A", "3"));
                copyCols->push_back(TableColumn("A", "4"));
            }

            TableColumns inputCols("A", "1", "2");

            applyMethod = makeApplyMethod("exec", "someMethodName", "outputTable", "outputColumn", inputCols, copyCols);
        }

        // translate
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();
        ops->push_back(applyMethod);

        buildLogicalPlan(ops)->apply(
                [&](LogicalPlan logicalPlan)
                {
                    // check translation
                    ComputationPtr computation = logicalPlan.getComputations().getProducingComputation("outputTable");
                    QUNIT_IS_EQUAL("Apply", computation->getComputationName());

                    shared_ptr<ApplyLambda> apply = dynamic_pointer_cast<ApplyLambda> (computation);

                    QUNIT_IS_EQUAL("exec", apply->getLambdaToApply());
                    QUNIT_IS_EQUAL("A", apply->getInputName());
                    QUNIT_IS_EQUAL("outputTable", apply->getOutputName());

                    QUNIT_IS_EQUAL(3, apply->getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("3", apply->getOutput().getAtts()[0]);
                    QUNIT_IS_EQUAL("4", apply->getOutput().getAtts()[1]);
                    QUNIT_IS_EQUAL("outputColumn", apply->getOutput().getAtts()[2]);

                    QUNIT_IS_EQUAL(2, apply->getInput().getAtts().size());
                    QUNIT_IS_EQUAL("1", apply->getInput().getAtts()[0]);
                    QUNIT_IS_EQUAL("2", apply->getInput().getAtts()[1]);

                    QUNIT_IS_EQUAL(2, apply->getProjection().getAtts().size());
                    QUNIT_IS_EQUAL("3", apply->getProjection().getAtts()[0]);
                    QUNIT_IS_EQUAL("4", apply->getProjection().getAtts()[1]);
                },
                [&](string errorMsg)
                {
                    QUNIT_IS_TRUE(false);
                });



    }

    void testBuildLogicalPlanFromHoist(UnitTest &qunit)
    {
        // the instruction to translate
        HoistPtr hoist;
        {
            shared_ptr<vector<TableColumn>> copyCols = make_shared<vector<TableColumn>>();
            {
                copyCols->push_back(TableColumn("A", "3"));
                copyCols->push_back(TableColumn("A", "4"));
            }

            hoist = makeHoist("fieldId", TableColumn("inputTable", "InputColumn"), TableColumn("outputTable", "outputColumn"),
                              copyCols, "exec");
        }

        // translate
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();
        ops->push_back(hoist);

        buildLogicalPlan(ops)->apply(
                [&](LogicalPlan logicalPlan)
                {
                    // check translation
                    ComputationPtr computation = logicalPlan.getComputations().getProducingComputation("outputTable");
                    QUNIT_IS_EQUAL("Apply", computation->getComputationName());

                    shared_ptr<ApplyLambda> apply = dynamic_pointer_cast<ApplyLambda> (computation);
                    QUNIT_IS_EQUAL("exec", apply->getLambdaToApply());
                    QUNIT_IS_EQUAL("inputTable", apply->getInputName());
                    QUNIT_IS_EQUAL("outputTable", apply->getOutputName());

                    QUNIT_IS_EQUAL(3, apply->getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("3", apply->getOutput().getAtts()[0]);
                    QUNIT_IS_EQUAL("4", apply->getOutput().getAtts()[1]);
                    QUNIT_IS_EQUAL("outputColumn", apply->getOutput().getAtts()[2]);

                    QUNIT_IS_EQUAL(1, apply->getInput().getAtts().size());
                    QUNIT_IS_EQUAL("InputColumn", apply->getInput().getAtts()[0]);

                    QUNIT_IS_EQUAL(2, apply->getProjection().getAtts().size());
                    QUNIT_IS_EQUAL("3", apply->getProjection().getAtts()[0]);
                    QUNIT_IS_EQUAL("4", apply->getProjection().getAtts()[1]);
                },
                [&](string errorMsg)
                {
                    QUNIT_IS_TRUE(false);
                });
    }

    void testBuildLogicalPlanFromGreaterThan(UnitTest &qunit)
    {
        // the instruction to translate
        GreaterThanPtr greaterThan;
        {
            shared_ptr<vector<TableColumn>> copyCols = make_shared<vector<TableColumn>>();
            {
                copyCols->push_back(TableColumn("A", "3"));
                copyCols->push_back(TableColumn("A", "4"));
            }

            greaterThan = makeGreaterThan(TableColumn("A", "1"), TableColumn("A", "2"), TableColumn("outputTable", "isGreater"),
                                          copyCols, "exec");
        }

        // translate
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();

        ops->push_back(greaterThan);

        buildLogicalPlan(ops)->apply(
                [&](LogicalPlan logicalPlan)
                {
                    // check translation
                    ComputationPtr computation = logicalPlan.getComputations().getProducingComputation("outputTable");
                    QUNIT_IS_EQUAL("Apply", computation->getComputationName());

                    shared_ptr<ApplyLambda> apply = dynamic_pointer_cast<ApplyLambda> (computation);
                    QUNIT_IS_EQUAL("exec", apply->getLambdaToApply());
                    QUNIT_IS_EQUAL("A", apply->getInputName());
                    QUNIT_IS_EQUAL("outputTable", apply->getOutputName());

                    QUNIT_IS_EQUAL(3, apply->getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("3", apply->getOutput().getAtts()[0]);
                    QUNIT_IS_EQUAL("4", apply->getOutput().getAtts()[1]);
                    QUNIT_IS_EQUAL("isGreater", apply->getOutput().getAtts()[2]);

                    QUNIT_IS_EQUAL(2, apply->getInput().getAtts().size());
                    QUNIT_IS_EQUAL("1", apply->getInput().getAtts()[0]);
                    QUNIT_IS_EQUAL("2", apply->getInput().getAtts()[1]);

                    QUNIT_IS_EQUAL(2, apply->getProjection().getAtts().size());
                    QUNIT_IS_EQUAL("3", apply->getProjection().getAtts()[0]);
                    QUNIT_IS_EQUAL("4", apply->getProjection().getAtts()[1]);
                },
                [&](string errorMsg)
                {
                    QUNIT_IS_TRUE(false);
                });
    }

    void testBuildLogicalPlanFromFilter(UnitTest &qunit)
    {
        // the instruction to translate
        FilterPtr filter;
        {
            shared_ptr<vector<TableColumn>> copyCols = make_shared<vector<TableColumn>>();
            {
                copyCols->push_back(TableColumn("inputTable", "student"));
            }

            filter = makeFilter("inputTable", "filterColumn", "outputTable", copyCols);
        }

        // translate
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();

        ops->push_back(filter);

        buildLogicalPlan(ops)->apply(
                [&](LogicalPlan logicalPlan)
                {
                    // check translation
                    ComputationPtr computation = logicalPlan.getComputations().getProducingComputation("outputTable");
                    QUNIT_IS_EQUAL("Filter", computation->getComputationName());

                    shared_ptr<ApplyFilter> applyFilter = dynamic_pointer_cast<ApplyFilter> (computation);


                    QUNIT_IS_EQUAL("inputTable", applyFilter->getInputName());
                    QUNIT_IS_EQUAL(1, applyFilter->getInput().getAtts().size());
                    QUNIT_IS_EQUAL("filterColumn", applyFilter->getInput().getAtts()[0]);

                    QUNIT_IS_EQUAL("outputTable", applyFilter->getOutputName());
                    QUNIT_IS_EQUAL(1, applyFilter->getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("student", applyFilter->getOutput().getAtts()[0]);

                    QUNIT_IS_EQUAL(1, applyFilter->getProjection().getAtts().size());
                    QUNIT_IS_EQUAL("student", applyFilter->getProjection().getAtts()[0]);
                },
                [&](string errorMsg)
                {
                    QUNIT_IS_TRUE(false);
                });

    }

    void testBuildLogicalPlanFromStore(UnitTest &qunit)
    {
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();

        shared_ptr<vector<string>> columnsToStore = make_shared<vector<string>>();
        columnsToStore->push_back("1");
        columnsToStore->push_back("2");

        ops->push_back(makeStore(TableColumns("inputTable", columnsToStore), "db set"));

        buildLogicalPlan(ops)->apply(
                [&](LogicalPlan logicalPlan)
                {
                    OutputList outputs = logicalPlan.getOutputs();
                    QUNIT_IS_EQUAL("db", outputs.getConsumers("inputTable").operator[](0).getdbName());
                    QUNIT_IS_EQUAL("set", outputs.getConsumers("inputTable").operator[](0).getSetName());
                    QUNIT_IS_EQUAL("inputTable", outputs.getConsumers("inputTable").operator[](0).getInputName());
                    QUNIT_IS_EQUAL(2, outputs.getConsumers("inputTable").operator[](0).getInput().getAtts().size());
                    QUNIT_IS_EQUAL("1", outputs.getConsumers("inputTable").operator[](0).getInput().getAtts()[0]);
                    QUNIT_IS_EQUAL("2", outputs.getConsumers("inputTable").operator[](0).getInput().getAtts()[1]);
                },
                [&](string errorMsg)
                {
                    QUNIT_IS_TRUE(false);
                });



    }

    void testBuildLogicalPlanFromStringTest47(UnitTest &qunit)
    {
        string program = "A(a) = load \"myDB mySet\"\n"
                         "@exec \"attAccess_2\"\n"
                         "B(a,b) = hoist \"someAtt\" from A[a] retain all\n"
                         "@exec \"methodCall_1\"\n"
                         "C(a,b,c) = apply method \"someMethod\" to B[a] retain all\n"
                         "@exec \"==_0\"\n"
                         "D(a,b) = apply func \"someFunc\" to C[c,b] retain a\n"
                         "E(a) = filter D by b retain a\n"
                         "@exec \"methodCall_3\"\n"
                         "F(a,b) = apply method \"someMethod\" to E[a] retain a\n"
                         "store F[b] \"myDB mySet\"";

        shared_ptr<SafeResult<LogicalPlan>> logicalPlanResult = buildLogicalPlan(program);

        logicalPlanResult->apply(
                [&](LogicalPlan logicalPlan)
                {


                    QUNIT_IS_EQUAL(1, logicalPlan.getInputs().size());
                    Input input = logicalPlan.getInputs().getInput("A");
                    QUNIT_IS_EQUAL("myDB", input.getDbName());
                    QUNIT_IS_EQUAL("mySet", input.getSetName());
                    QUNIT_IS_EQUAL("A", input.getOutputName());
                    QUNIT_IS_EQUAL(1, input.getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("a", input.getOutput().getAtts()[0]);

                    ComputationPtr computation = logicalPlan.getComputations().getProducingComputation("B");
                    QUNIT_IS_EQUAL("Apply", computation->getComputationName());
                    shared_ptr<ApplyLambda> apply = dynamic_pointer_cast<ApplyLambda>(computation);
                    QUNIT_IS_EQUAL("attAccess_2", apply->getLambdaToApply());
                    QUNIT_IS_EQUAL("B", apply->getOutputName());
                    QUNIT_IS_EQUAL("2", apply->getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getOutput().getAtts()[0]);
                    QUNIT_IS_EQUAL("b", apply->getOutput().getAtts()[1]);
                    QUNIT_IS_EQUAL("A", apply->getInputName());
                    QUNIT_IS_EQUAL("1", apply->getInput().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getInput().getAtts()[0]);
                    QUNIT_IS_EQUAL("A", apply->getProjection().getSetName());
                    QUNIT_IS_EQUAL("1", apply->getProjection().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getProjection().getAtts()[0]);


                    computation = logicalPlan.getComputations().getProducingComputation("C");
                    QUNIT_IS_EQUAL("Apply", computation->getComputationName());
                    apply = dynamic_pointer_cast<ApplyLambda>(computation);
                    QUNIT_IS_EQUAL("methodCall_1", apply->getLambdaToApply());
                    QUNIT_IS_EQUAL("C", apply->getOutputName());
                    QUNIT_IS_EQUAL("3", apply->getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getOutput().getAtts()[0]);
                    QUNIT_IS_EQUAL("b", apply->getOutput().getAtts()[1]);
                    QUNIT_IS_EQUAL("c", apply->getOutput().getAtts()[2]);
                    QUNIT_IS_EQUAL("B", apply->getInputName());
                    QUNIT_IS_EQUAL("1", apply->getInput().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getInput().getAtts()[0]);
                    QUNIT_IS_EQUAL("B", apply->getProjection().getSetName());
                    QUNIT_IS_EQUAL("2", apply->getProjection().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getProjection().getAtts()[0]);
                    QUNIT_IS_EQUAL("b", apply->getProjection().getAtts()[1]);

                    computation = logicalPlan.getComputations().getProducingComputation("D");
                    QUNIT_IS_EQUAL("Apply", computation->getComputationName());
                    apply = dynamic_pointer_cast<ApplyLambda>(computation);
                    QUNIT_IS_EQUAL("==_0", apply->getLambdaToApply());
                    QUNIT_IS_EQUAL("D", apply->getOutputName());
                    QUNIT_IS_EQUAL("2", apply->getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getOutput().getAtts()[0]);
                    QUNIT_IS_EQUAL("b", apply->getOutput().getAtts()[1]);
                    QUNIT_IS_EQUAL("C", apply->getInputName());
                    QUNIT_IS_EQUAL("2", apply->getInput().getAtts().size());
                    QUNIT_IS_EQUAL("c", apply->getInput().getAtts()[0]);
                    QUNIT_IS_EQUAL("b", apply->getInput().getAtts()[1]);
                    QUNIT_IS_EQUAL("C", apply->getProjection().getSetName());
                    QUNIT_IS_EQUAL("1", apply->getProjection().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getProjection().getAtts()[0]);

                    computation = logicalPlan.getComputations().getProducingComputation("E");
                    QUNIT_IS_EQUAL("Filter", computation->getComputationName());
                    shared_ptr<ApplyFilter> filter = dynamic_pointer_cast<ApplyFilter>(computation);
                    QUNIT_IS_EQUAL("E", filter->getOutputName());
                    QUNIT_IS_EQUAL("1", filter->getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("a", filter->getOutput().getAtts()[0]);
                    QUNIT_IS_EQUAL("D", filter->getInputName());
                    QUNIT_IS_EQUAL("1", filter->getInput().getAtts().size());
                    QUNIT_IS_EQUAL("b", filter->getInput().getAtts()[0]);
                    QUNIT_IS_EQUAL("D", filter->getProjection().getSetName());
                    QUNIT_IS_EQUAL("1", filter->getProjection().getAtts().size());
                    QUNIT_IS_EQUAL("a", filter->getProjection().getAtts()[0]);

                    computation = logicalPlan.getComputations().getProducingComputation("F");
                    QUNIT_IS_EQUAL("Apply", computation->getComputationName());
                    apply = dynamic_pointer_cast<ApplyLambda>(computation);
                    QUNIT_IS_EQUAL("methodCall_3", apply->getLambdaToApply());
                    QUNIT_IS_EQUAL("F", apply->getOutputName());
                    QUNIT_IS_EQUAL("2", apply->getOutput().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getOutput().getAtts()[0]);
                    QUNIT_IS_EQUAL("b", apply->getOutput().getAtts()[1]);
                    QUNIT_IS_EQUAL("E", apply->getInputName());
                    QUNIT_IS_EQUAL("1", apply->getInput().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getInput().getAtts()[0]);
                    QUNIT_IS_EQUAL("E", apply->getProjection().getSetName());
                    QUNIT_IS_EQUAL("1", apply->getProjection().getAtts().size());
                    QUNIT_IS_EQUAL("a", apply->getProjection().getAtts()[0]);

                    QUNIT_IS_EQUAL(1, logicalPlan.getOutputs().getConsumers("F").size());
                    Output output = logicalPlan.getOutputs().getConsumers("F")[0];
                    QUNIT_IS_EQUAL("myDB", output.getdbName());
                    QUNIT_IS_EQUAL("mySet", output.getSetName());
                    QUNIT_IS_EQUAL("F", output.getInputName());
                    QUNIT_IS_EQUAL(1, output.getInput().getAtts().size());
                    QUNIT_IS_EQUAL("b", output.getInput().getAtts()[0]);
                },
                [&](string error)
                {
                    QUNIT_IS_TRUE(false);
                });
    }
}
