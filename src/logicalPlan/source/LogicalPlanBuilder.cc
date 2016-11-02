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
#include "LogicalPlanBuilder.h"

#include <sstream>
#include <iterator>

#include "ApplyBase.h"
#include "ApplyFunction.h"
#include "ApplyMethod.h"
#include "Filter.h"
#include "Load.h"
#include "GreaterThan.h"
#include "Hoist.h"
#include "Instruction.h"
#include "Store.h"
#include "TableColumns.h"

using std::istringstream;
using std::istream_iterator;

using pdb_detail::ApplyBase;
using pdb_detail::ApplyFunction;
using pdb_detail::ApplyMethod;
using pdb_detail::Filter;
using pdb_detail::Load;
using pdb_detail::GreaterThan;
using pdb_detail::Hoist;
using pdb_detail::Store;
using pdb_detail::TableColumns;

/**
 * @return an attribute list composed of each of the given column's id, in the same order as provided.
 */
AttList makeAttributeList(const shared_ptr<vector<Column>> &columns)
{
    AttList list;

    for(const Column& column : *columns.get())
    {
        list.appendAttribute(column.columnId);
    }

    return list;
}

/**
 * @return an attribute list composed of each of the given column's id, in the same order as provided.
 */
AttList makeAttributeList(const TableColumns &columns)
{
    AttList list;

    for(const string& column : *columns.columnIds.get())
    {
        list.appendAttribute(column);
    }

    return list;
}

/**
 * Creates a corresponding ApplyLambda from the given ApplyBase.
 *
 * For example, if the given ApplyBase modeled the following TCAP statement:
 *
 *     @exec "executor1"
 *     B(student, teacher, fooResult) = apply func "foo" to A[student,class] retain student, teacher
 *
 * The translated ApplLambda woud be constructed with the following parameters:
 *
 * input:        ("A", ["student", "class"])
 * output:       ("B", ["student", "teacher", "fooResult"])
 * projection:   ("A", ["student", "teacher"])
 * lambdaNameIn: "executor1"
 *
 * @param apply the ApplyBase to translate
 * @return an ApplyLambda that corresponds to apply.
 */
shared_ptr<ApplyLambda> makeApplyLambdaFromApplyBase(const ApplyBase &apply)
{
    /*
     * Create an "input" TupleSpec that corresponds to ApplyBase.inputColumns;
     *
     * For example, if the original TCAP assignment was:
     *
     *     B(student, teacher, fooResult) = apply func "foo" to A[student,class] retain student, teacher
     *
     * input would be:
     *
     *    setName: "A"
     *    atts: ["student", "class"]
     */
    TupleSpec input;
    {
        AttList attributes = makeAttributeList(apply.inputColumns);
        string inputTable = apply.inputColumns.tableName;
        input = TupleSpec(inputTable, attributes);
    }

    /*
     * Create an "output" TupleSpec that corresponds to the result of the apply operation
     *
     * For example, if the original TCAP assignment was:
     *
     *     B(student, teacher, fooResult) = apply func "foo" to A[student,class] retain student, teacher
     *
     * output would be:
     *
     *    setName: "B"
     *    atts: ["student", "teacher", "fooResult"]
     */
    TupleSpec output;
    {
        AttList attributes = makeAttributeList(apply.columnsToCopyToOutputTable);
        attributes.appendAttribute(apply.outputColumnId);
        output = TupleSpec(apply.outputTableId, attributes);
    }

    /*
     * Create a "projection" TupleSpec that describes which columns are to be copied from the input to the output.
     * (these will be a subset of the output TupleSpec created above)
     *
     * For example, if the original TCAP assignment was:
     *
     *     B(student, teacher, fooResult) = apply func "foo" to A[student,class] retain student, teacher
     *
     * output would be:
     *
     *    setName: "A"
     *    atts: ["student", "teacher"]
     *
     */
    TupleSpec projection;
    {
        AttList attributes = makeAttributeList(apply.columnsToCopyToOutputTable);
        projection = TupleSpec(apply.inputColumns.tableName, attributes);
    }

    return make_shared<ApplyLambda>(input, output, projection, apply.executorId);
}

/**
 * Creates a corresponding Input from the given Load:
 *
 * For example, if the given Load modeled the following TCAP statement:
 *
 *      A(student) = load "db set"
 *
 * the created Input would have the following form:
 *
 *     output:  ("A", ["student"])
 *     dbName:  "db"
 *     setName: "set"
 *
 * To form "dbName" and "setName", the entire string literal is tokenized by whitespace and the first token
 * is used for "dbName" and the second for "setName".  If the tokenization of load.source results in anything
 * but exactly two tokens, a string exception is thrown.
 *
 * @param load the Load to translate into an Input.
 * @return An Input corresponding to load.
 */
Input makeInputFromLoad(const Load &load)
{
    TupleSpec outputTable;
    {
        AttList attList;
        attList.appendAttribute(load.outputColumnId);
        outputTable = TupleSpec(load.outputTableId, attList);
    }

    vector<string> sourceTokens; // tokenize load.source by whitespace
    {
        istringstream iss(load.source);
        copy(istream_iterator<string>(iss), istream_iterator<string>(), back_inserter(sourceTokens));

        if(sourceTokens.size() != 2)
            throw "unrecognized source string " + load.source;
    }

    return Input(outputTable, sourceTokens[0], sourceTokens[1]);

}

/**
 * Creates a corresponding ApplyFilter from the given Filter:
 *
 * For example, if the given Filter modeled the following TCAP statement:
 *
 *      E(student, teacher) = filter D by isExamGreater retain student, teacher
 *
 * the created ApplyFilter would have the following form:
 *
 * input:        ("D", ["isExamGreater"])
 * output:       ("E", ["student", "teacher"])
 * projection:   ("D", ["student", "teacher"])
 *
 * @param filter the Filter to translate into an ApplyFilter
 * @return an ApplyFilter corresponding to filter
 */
shared_ptr<ApplyFilter> makeApplyFilterFromFilter(const Filter &filter)
{
    /*
     * Create an "input" TupleSpec that corresponds to hoist.inputColumn;
     *
     * For example, if the original TCAP assignment was:
     *
     *     E(student, teacher) = filter D by isExamGreater retain student, teacher
     *
     * input would be:
     *
     *    setName: "D"
     *    atts: ["isExamGreater"]
     */
    TupleSpec input;
    {
        AttList filterColumn;
        filterColumn.appendAttribute(filter.filterColumnId);
        input = TupleSpec(filter.inputTableId, filterColumn);
    }

    /*
     * Create an "output" TupleSpec that corresponds the result of the operation.
     *
     * For example, if the original TCAP assignment was:
     *
     *     E(student, teacher) = filter D by isExamGreater retain student, teacher
     *
     * output would be:
     *
     *    setName: "E"
     *    atts: ["student", "teacher"]
     */
    TupleSpec output;
    {
        AttList attributes = makeAttributeList(filter.columnsToCopyToOutputTable);
        output = TupleSpec(filter.outputTableId, attributes);
    }

    /*
     * Create a "projection" TupleSpec that describes which columns are to be copied from the input to the output.
     * (these will be a subset of the output TupleSpec created above)
     *
     * For example, if the original TCAP assignment was:
     *
     *    E(student, teacher) = filter D by isExamGreater retain student, teacher
     *
     * output would be:
     *
     *    setName: "D"
     *    atts: ["student", "teacher"]
     *
     */
    TupleSpec projection;
    {
        AttList attributes = makeAttributeList(filter.columnsToCopyToOutputTable);
        projection = TupleSpec(filter.inputTableId, attributes);
    }

    return make_shared<ApplyFilter>(input, output, projection);
}

/**
 * Creates a corresponding ApplyLambda from the given Hoist:
 *
 * For example, if the given Hoist modeled the following TCAP statement:
 *
 *    @exec "executor2"
 *    C(student, examAverage, hwAverage) = hoist "homeworkAverage" from B[student] retain student, examAverage
 *
 * the created ApplyLambda would have the following form:
 *
 * input:      ("B", ["student"])
 * output:     ("C", ["student", "examAverage", "hwAverage"])
 * projection: ("B", ["student", "examAverage"])
 * lambdaName: "executor2"
 *
 * @param hoist the Hoist to translate into an ApplyLambda
 * @return an ApplyLambda translation of host
 */
shared_ptr<ApplyLambda> makeApplyLambdaFromHoist(const Hoist &hoist)
{
    /*
     * Create an "input" TupleSpec that corresponds to hoist.inputColumn;
     *
     * For example, if the original TCAP assignment was:
     *
     *     C(student, examAverage, hwAverage) = hoist "homeworkAverage" from B[student] retain student, examAverage
     *
     * input would be:
     *
     *    setName: "B"
     *    atts: ["student"]
     */
    TupleSpec input;
    {
        AttList inputColumn;
        inputColumn.appendAttribute(hoist.inputColumn.columnId);
        input = TupleSpec(hoist.inputColumn.tableId, inputColumn);
    }

    /*
     * Create an "output" TupleSpec that corresponds the result of the operation.
     *
     * For example, if the original TCAP assignment was:
     *
     *     C(student, examAverage, hwAverage) = hoist "homeworkAverage" from B[student] retain student, examAverage
     *
     * output would be:
     *
     *    setName: "C"
     *    atts: ["student", "examAverage", "hwAverage"]
     */
    TupleSpec output;
    {
        AttList attributes = makeAttributeList(hoist.columnsToCopyToOutputTable);
        attributes.appendAttribute(hoist.outputColumn.columnId);
        output = TupleSpec(hoist.outputColumn.tableId, attributes);
    }

    /*
     * Create a "projection" TupleSpec that describes which columns are to be copied from the input to the output.
     * (these will be a subset of the output TupleSpec created above)
     *
     * For example, if the original TCAP assignment was:
     *
     *     C(student, examAverage, hwAverage) = hoist "homeworkAverage" from B[student] retain student, examAverage
     *
     * output would be:
     *
     *    setName: "B"
     *    atts: ["student", "examAverage"]
     *
     */
    TupleSpec projection;
    {
        AttList attributes = makeAttributeList(hoist.columnsToCopyToOutputTable);
        projection = TupleSpec(hoist.inputColumn.tableId, attributes);
    }

    return make_shared<ApplyLambda>(input, output, projection, hoist.executorId);
}

/**
 * Creates a corresponding ApplyLambda from the given GreaterThan:
 *
 * For example, if the given GreaterThan modeled the following TCAP statement:
 *
 *    @exec "executor3"
 *    D(student, isExamGreater) = C[examAverage] > C[hwAverage] retain student
 *
 * the created ApplyLambda would have the following form:
 *
 * input:      ("C", ["examAverage", "hwAverage"])
 * output:     ("D", ["student", "isExamGreater", "hwAverage"])
 * projection: ("C", ["student"])
 * lambdaName: "executor3"
 *
 * The order of the columns in input is guranteed to be first the left hand side column followed by the right.
 * ApplyLambda only supports the left hand column and right hand column coming from the same set, so if the
 * table ids for the left hand side and right hand side don't match this method willl throw a string exception.
 *
 * @param gt the GreaterThan to translate into an ApplyLambda
 * @return and ApplyLambda representatino of gt
 */
shared_ptr<ApplyLambda> makeApplyLambdaFromGreaterThan(const GreaterThan &gt)
{
    if(gt.leftHandSide.tableId != gt.rightHandSide.tableId)
        throw "cross table comparison not supported by ApplyLambda"; // TupleSpec allows only one setNameIn in constructor

    string inputTableId = gt.leftHandSide.tableId;

    /*
     * Create an "input" TupleSpec that corresponds the left and right hand sides of gt while preserving order.
     *
     * For example, if the original TCAP assignment was:
     *
     *     D(student, isExamGreater) = C[examAverage] > C[hwAverage] retain student
     *
     * input would be:
     *
     *    setName: "C"
     *    atts: ["examAverage", "hwAverage"]
     */
    TupleSpec input;
    {
        AttList inputColumn;
        inputColumn.appendAttribute(gt.leftHandSide.columnId);
        inputColumn.appendAttribute(gt.rightHandSide.columnId);
        input = TupleSpec(inputTableId, inputColumn);
    }

    /*
     * Create an "output" TupleSpec that corresponds the result of the operation.
     *
     * For example, if the original TCAP assignment was:
     *
     *    D(student, isExamGreater) = C[examAverage] > C[hwAverage] retain student
     *
     * output would be:
     *
     *    setName: "C"
     *    atts: ["student", "isExamGreater"]
     */
    TupleSpec output;
    {
        AttList attributes = makeAttributeList(gt.columnsToCopyToOutputTable);
        attributes.appendAttribute(gt.outputColumn.columnId);
        output = TupleSpec(gt.outputColumn.tableId, attributes);
    }

    /*
     * Create a "projection" TupleSpec that describes which columns are to be copied from the input to the output.
     * (these will be a subset of the output TupleSpec created above)
     *
     * For example, if the original TCAP assignment was:
     *
     *     D(student, isExamGreater) = C[examAverage] > C[hwAverage] retain student
     *
     * output would be:
     *
     *    setName: "C"
     *    atts: ["student"]
     *
     */
    TupleSpec projection;
    {
        AttList attributes = makeAttributeList(gt.columnsToCopyToOutputTable);
        projection = TupleSpec(inputTableId, attributes);
    }

   return make_shared<ApplyLambda>(input, output, projection, gt.executorId);

}

/**
 * Creates a corresponding Output from the given Store:
 *
 * For example, if the given GreaterThan modeled the following TCAP statement:
 *
 *     store F "db set"
 *
 * the created Output would have the following form:
 *
 *     input:   ("F", [])
 *     dbName:  "db"
 *     setName: "set"
 *
 * To form "dbName" and "setName", the entire string literal is tokenized by whitespace and the first token
 * is used for "dbName" and the second for "setName".  If the tokenization of store.destination results in anything
 * but exactly two tokens, a string exception is thrown.
 *
 * @param store the Store to translate into an Ouput
 * @return an Ouput corresponding to store
 */
Output makeOutputFromStore(const Store &store)
{
    vector<string> destinationTokens; // tokenize store.destination by whitespace
    {
        istringstream iss(store.destination);
        copy(istream_iterator<string>(iss), istream_iterator<string>(), back_inserter(destinationTokens));

        if(destinationTokens.size() != 2)
            throw "unrecognized source string " + store.destination;
    }

    AttList empty;
    TupleSpec toStore(store.tableId, empty);

    Output out(toStore, destinationTokens[0], destinationTokens[1]);

    return out;
}

// contract from .h
shared_ptr<LogicalPlan> buildLogicalPlan(shared_ptr<vector<InstructionPtr>> instructions)
{
    /*
     * Translate every IR instruction in instructions into a corresponding Logical Plan representation.
     *
     * Load turns into Input
     * Store turns into Output
     * everything else turns into a Computation.
     *
     * Collect the translated structures into outputs, inputs, and compList.
     */
    OutputList outputsAccum;
    InputList inputsAccum;
    ComputationList compListAccum;

    for(shared_ptr<Instruction> instruction : *instructions.get())
    {
        bool exceptionGeneratedInsideMatch = false;

        instruction->match(
                [&](Load &load)
                {
                    try
                    {
                        Input input = makeInputFromLoad(load);
                        inputsAccum.addInput(input);
                    }
                    catch(string &errorMsg)
                    {
                        exceptionGeneratedInsideMatch = true;
                    }
                },
                [&](ApplyFunction &applyFunction)
                {
                    shared_ptr<ApplyLambda> applyLambda = makeApplyLambdaFromApplyBase(applyFunction);
                    compListAccum.addComputation(applyLambda);
                },
                [&](ApplyMethod &applyMethod)
                {
                    shared_ptr<ApplyLambda> applyLambda = makeApplyLambdaFromApplyBase(applyMethod);
                    compListAccum.addComputation(applyLambda);
                },
                [&](Filter &filter)
                {
                    shared_ptr<ApplyFilter> applyFilter = makeApplyFilterFromFilter(filter);
                    compListAccum.addComputation(applyFilter);
                },
                [&](Hoist &hoist)
                {
                    shared_ptr<ApplyLambda> applyLambda = makeApplyLambdaFromHoist(hoist);
                    compListAccum.addComputation(applyLambda);
                },
                [&](GreaterThan &gt)
                {
                    try
                    {
                        shared_ptr<ApplyLambda> applyLambda = makeApplyLambdaFromGreaterThan(gt);
                        compListAccum.addComputation(applyLambda);
                    }
                    catch(string &errorMsg)
                    {
                        exceptionGeneratedInsideMatch = true;
                    }
                },
                [&](Store &store)
                {
                    Output out = makeOutputFromStore(store);
                    outputsAccum.addOutput(out);
                });

        if(exceptionGeneratedInsideMatch) // PDB coding rules prevent exceptions from crossing API boundaries.
            return nullptr;               // so return a nullptr instead of propagating an exception
    }

    return make_shared<LogicalPlan>(outputsAccum,inputsAccum,compListAccum);
}

