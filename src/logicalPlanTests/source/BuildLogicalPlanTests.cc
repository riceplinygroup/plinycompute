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
using pdb_detail::ApplyMethod;
using pdb_detail::Filter;
using pdb_detail::GreaterThan;
using pdb_detail::Hoist;
using pdb_detail::Load;
using pdb_detail::Store;
using pdb_detail::TableColumns;

namespace pdb_tests
{
    void testBuildLoad(UnitTest &qunit)
    {
        shared_ptr<vector<shared_ptr<Instruction>>> ops = make_shared<vector<shared_ptr<Instruction>>>();

        ops->push_back(make_shared<Load>("table", "column", "1 2"));

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

    void testBuildApplyFunction(UnitTest &qunit)
    {
        shared_ptr<vector<shared_ptr<Instruction>>> ops = make_shared<vector<shared_ptr<Instruction>>>();

        TableColumns inputCols("A", "1", "2");

        shared_ptr<vector<Column>> copyCols = make_shared<vector<Column>>();
        {
            copyCols->push_back(Column("A", "3"));
            copyCols->push_back(Column("A", "4"));
        }

        ops->push_back(make_shared<ApplyFunction>("exec", "function", "outputTable", "outputColumn", inputCols, copyCols));

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

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

    void testBuildApplyMethod1(UnitTest &qunit)
    {
        shared_ptr<vector<shared_ptr<Instruction>>> ops = make_shared<vector<shared_ptr<Instruction>>>();

        TableColumns inputCols("A", "1", "2");

        shared_ptr<vector<Column>> copyCols = make_shared<vector<Column>>();
        {
            copyCols->push_back(Column("A", "3"));
            copyCols->push_back(Column("A", "4"));
        }

        ops->push_back(make_shared<ApplyMethod>("exec", "function", "outputTable", "outputColumn", inputCols, copyCols));

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

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

    void testBuildApplyMethod2(UnitTest &qunit)
    {
        shared_ptr<vector<shared_ptr<Instruction>>> ops = make_shared<vector<shared_ptr<Instruction>>>();


        shared_ptr<vector<Column>> copyCols = make_shared<vector<Column>>();
        {
            copyCols->push_back(Column("A", "3"));
            copyCols->push_back(Column("A", "4"));
        }

        ops->push_back(make_shared<Hoist>("fieldId", Column("inputTable", "InputColumn"), Column("outputTable", "outputColumn"), copyCols, "exec"));

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

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

    void testBuildApplyMethod3(UnitTest &qunit)
    {
        shared_ptr<vector<shared_ptr<Instruction>>> ops = make_shared<vector<shared_ptr<Instruction>>>();

        shared_ptr<vector<Column>> copyCols = make_shared<vector<Column>>();
        {
            copyCols->push_back(Column("A", "3"));
            copyCols->push_back(Column("A", "4"));
        }

        ops->push_back(make_shared<GreaterThan>(Column("A", "1"), Column("A", "2"), Column("outputTable", "isGreater"), copyCols, "exec"));

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

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

    void testBuildApplyFilter(UnitTest &qunit)
    {
        shared_ptr<vector<shared_ptr<Instruction>>> ops = make_shared<vector<shared_ptr<Instruction>>>();

        shared_ptr<vector<Column>> copyCols = make_shared<vector<Column>>();
        {
            copyCols->push_back(Column("inputTable", "student"));
        }

        ops->push_back(make_shared<Filter>("inputTable", "filterColumn", "outputTable", copyCols));

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

        ComputationPtr computation = logicalPlan->getComputations().getProducingComputation("outputTable");
        QUNIT_IS_EQUAL("Filter", computation->getComputationName());

        shared_ptr<ApplyFilter> filter = dynamic_pointer_cast<ApplyFilter> (computation);


        QUNIT_IS_EQUAL("inputTable", filter->getInputName());
        QUNIT_IS_EQUAL(1, filter->getInput().getAtts().size());
        QUNIT_IS_EQUAL("filterColumn", filter->getInput().getAtts()[0]);

        QUNIT_IS_EQUAL("outputTable", filter->getOutputName());
        QUNIT_IS_EQUAL(1, filter->getOutput().getAtts().size());
        QUNIT_IS_EQUAL("student", filter->getOutput().getAtts()[0]);

        QUNIT_IS_EQUAL(1, filter->getProjection().getAtts().size());
        QUNIT_IS_EQUAL("student", filter->getProjection().getAtts()[0]);

    }

    void testBuildStore(UnitTest &qunit)
    {
        shared_ptr<vector<shared_ptr<Instruction>>> ops = make_shared<vector<shared_ptr<Instruction>>>();

        ops->push_back(make_shared<Store>("inputTable", "db set"));

        shared_ptr<LogicalPlan> logicalPlan = buildLogicalPlan(ops);

        OutputList outputs = logicalPlan->getOutputs();
        QUNIT_IS_EQUAL("db", outputs.getConsumers("inputTable").operator[](0).getdbName());
        QUNIT_IS_EQUAL("set", outputs.getConsumers("inputTable").operator[](0).getSetName())

    }
}
