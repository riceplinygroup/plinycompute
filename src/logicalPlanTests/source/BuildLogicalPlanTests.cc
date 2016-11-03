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
using pdb_detail::makeApplyFunction;
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

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

        QUNIT_IS_EQUAL(1, logicalPlan->getInputs().size());

        Input input = logicalPlan->getInputs().getInput("table");
        QUNIT_IS_EQUAL("table", input.getOutputName());
        QUNIT_IS_EQUAL("table", input.getOutput().getSetName());
        QUNIT_IS_EQUAL(1, input.getOutput().getAtts().size());
        QUNIT_IS_EQUAL("column", input.getOutput().getAtts()[0]);
        QUNIT_IS_EQUAL("1", input.getDbName());
        QUNIT_IS_EQUAL("2", input.getSetName());

    }

    void testBuildLogicalPlanFromApplyFunction(UnitTest &qunit)
    {
        // the instruction to translate
        ApplyFunctionPtr applyFunction;
        {
            shared_ptr<vector<Column>> copyCols = make_shared<vector<Column>>();
            {
                copyCols->push_back(Column("A", "3"));
                copyCols->push_back(Column("A", "4"));
            }

            TableColumns inputCols("A", "1", "2");

            applyFunction = makeApplyFunction("exec", "someFunctionName", "outputTable", "outputColumn", inputCols, copyCols);
        }

        // translate
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();
        ops->push_back(applyFunction);

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

        // check translation
        ComputationPtr computation = logicalPlan->getComputations().getProducingComputation("outputTable");

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

    }

    void testBuildLogicalPlanFromApplyMethod(UnitTest &qunit)
    {
        // the instruction to translate
        ApplyMethodPtr applyMethod;
        {
            shared_ptr<vector<Column>> copyCols = make_shared<vector<Column>>();
            {
                copyCols->push_back(Column("A", "3"));
                copyCols->push_back(Column("A", "4"));
            }

            TableColumns inputCols("A", "1", "2");

            applyMethod = makeApplyMethod("exec", "someMethodName", "outputTable", "outputColumn", inputCols, copyCols);
        }

        // translate
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();
        ops->push_back(applyMethod);

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

        // check translation
        ComputationPtr computation = logicalPlan->getComputations().getProducingComputation("outputTable");
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

    }

    void testBuildLogicalPlanFromHoist(UnitTest &qunit)
    {
        // the instruction to translate
        HoistPtr hoist;
        {
            shared_ptr<vector<Column>> copyCols = make_shared<vector<Column>>();
            {
                copyCols->push_back(Column("A", "3"));
                copyCols->push_back(Column("A", "4"));
            }

            hoist = makeHoist("fieldId", Column("inputTable", "InputColumn"), Column("outputTable", "outputColumn"),
                              copyCols, "exec");
        }

        // translate
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();
        ops->push_back(hoist);

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

        // check translation
        ComputationPtr computation = logicalPlan->getComputations().getProducingComputation("outputTable");
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

    }

    void testBuildLogicalPlanFromGreaterThan(UnitTest &qunit)
    {
        // the instruction to translate
        GreaterThanPtr greaterThan;
        {
            shared_ptr<vector<Column>> copyCols = make_shared<vector<Column>>();
            {
                copyCols->push_back(Column("A", "3"));
                copyCols->push_back(Column("A", "4"));
            }

            greaterThan = makeGreaterThan(Column("A", "1"), Column("A", "2"), Column("outputTable", "isGreater"),
                                          copyCols, "exec");
        }

        // translate
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();

        ops->push_back(greaterThan);

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

        // check translation
        ComputationPtr computation = logicalPlan->getComputations().getProducingComputation("outputTable");
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

    }

    void testBuildLogicalPlanFromFilter(UnitTest &qunit)
    {
        // the instruction to translate
        FilterPtr filter;
        {
            shared_ptr<vector<Column>> copyCols = make_shared<vector<Column>>();
            {
                copyCols->push_back(Column("inputTable", "student"));
            }

            filter = makeFilter("inputTable", "filterColumn", "outputTable", copyCols);
        }

        // translate
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();

        ops->push_back(filter);

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

        // check translation
        ComputationPtr computation = logicalPlan->getComputations().getProducingComputation("outputTable");
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

    }

    void testBuildLogicalPlanFromStore(UnitTest &qunit)
    {
        shared_ptr<vector<InstructionPtr>> ops = make_shared<vector<InstructionPtr>>();

        ops->push_back(makeStore("inputTable", "db set"));

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

        OutputList outputs = logicalPlan->getOutputs();
        QUNIT_IS_EQUAL("db", outputs.getConsumers("inputTable").operator[](0).getdbName());
        QUNIT_IS_EQUAL("set", outputs.getConsumers("inputTable").operator[](0).getSetName())

    }
}
