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
#ifndef PROLOG_GENERATOR_CC
#define PROLOG_GENERATOR_CC

#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <cctype>
#include <algorithm>
#include <unordered_set>

#include "Lexer.h"
#include "Parser.h"
#include "AtomicComputationList.h"
#include "AtomicComputation.h"
#include "AtomicComputationClasses.h"
#include "TupleSpec.h"

using namespace std;


// Store nodeNames into this:
unordered_set<string> nodeNames;

// Store linkNames into this:
unordered_set<string> linkNames;

// Store all prolog rules here:
vector<string> prologRules;


string getLowerCaseFirstLetter(string data) {
    data[0] = tolower(data[0]);
    return data;
}


void toLowerCaseFirstLetter(string& data) {
    data[0] = tolower(data[0]);
}

void toLowerCase(string& data) {
    transform(data.begin(), data.end(), data.begin(), ::tolower);
}


// In the Prolog rules data would appear as 'data'
void makePrologString(string& data) {
    data = "'" + data + "'";
}


void processLamdaName(string& data) {
    // data = "lambda_"  + data;
    makePrologString(data);
}


void parseAtomicComputation(AtomicComputationList* acl,
                            AtomicComputationPtr result,
                            string& inputName,
                            AtomicComputationPtr oldResult) {

    // Change "inputName" to lower case:
    toLowerCaseFirstLetter(inputName);

    // 1) Create the prolog rule "node":
    string computationType = result->getAtomicComputationType();
    toLowerCase(computationType);

    string outputName = result->getOutputName();
    toLowerCaseFirstLetter(outputName);

    string computationName = result->getComputationName();
    makePrologString(computationName);


    // Only print node rules if not previously done:
    unordered_set<string>::const_iterator findNode = nodeNames.find(outputName);
    if (findNode == nodeNames.end()) {
        // nodeNames.insert(outputName);

        // Instead of console pass it to a string stream:
        stringstream buffer;
        if (!computationType.compare("joinsets")) {
            buffer << "node(" << outputName << ", "
                   << "join"
                   << ", " << computationName << ")." << endl;
        } else if (!computationType.compare("writeset")) {
            buffer << "node(" << outputName << ", "
                   << "output"
                   << ", " << computationName << ")." << endl;
        } else {
            buffer << "node(" << outputName << ", " << computationType << ", " << computationName
                   << ")." << endl;
        }

        // Extract the prolog rule:
        string prologRule = buffer.str();

        // Save the created rule to print it later:
        prologRules.push_back(prologRule);
    }


    //*
    // 2) Create the prolog rule "link":
    string linkName = outputName + ", " + inputName;

    // Get required attributes (output, input and projection_:
    vector<string> outputAtts;
    vector<string> inputAtts;
    vector<string> projectionAtts;


    // Print a link only if has not been printed before:
    unordered_set<string>::const_iterator findLink = linkNames.find(linkName);
    if (findLink == linkNames.end()) {
        linkNames.insert(linkName);

        // Instead of console pass it to a string stream:
        stringstream buffer;

        buffer << "link(" << outputName << ", " << inputName << ", ";

        // Prepare output list:
        outputAtts = oldResult->getOutput().getAtts();
        buffer << "[";
        for (int i = 0; i < outputAtts.size(); i++) {
            toLowerCaseFirstLetter(outputAtts.at(i));
            buffer << outputAtts.at(i);
            if (i < (outputAtts.size() - 1)) {
                buffer << ", ";
            }
        }
        buffer << "], ";


        // Join:
        if (!computationType.compare("joinsets")) {
            string sourceComputationType = oldResult->getAtomicComputationType();
            toLowerCase(sourceComputationType);

            if (!sourceComputationType.compare("hashright")) {
                shared_ptr<ApplyJoin> newResult = dynamic_pointer_cast<ApplyJoin>(result);
                inputAtts = newResult->getRightInput().getAtts();
                projectionAtts = newResult->getRightProjection().getAtts();
            } else {
                inputAtts = result->getInput().getAtts();
                projectionAtts = result->getProjection().getAtts();
            }
        }
        // Other computations:
        else {
            inputAtts = result->getInput().getAtts();
            projectionAtts = result->getProjection().getAtts();
        }


        // Prepare input list:
        buffer << "[";
        for (int i = 0; i < inputAtts.size(); i++) {
            toLowerCaseFirstLetter(inputAtts.at(i));
            buffer << inputAtts.at(i);
            if (i < (inputAtts.size() - 1)) {
                buffer << ", ";
            }
        }
        buffer << "], ";


        // Prepare projection list:
        buffer << "[";
        for (int i = 0; i < projectionAtts.size(); i++) {
            toLowerCaseFirstLetter(projectionAtts.at(i));
            buffer << projectionAtts.at(i);
            if (i < (projectionAtts.size() - 1)) {
                buffer << ", ";
            }
        }
        buffer << "]";
        buffer << ")." << endl;


        // Extract the prolog rule:
        string prologRule = buffer.str();

        // Save the created rule to print it later:
        prologRules.push_back(prologRule);
    }

    //*/

    //*
    // 3) Create node specific rules:
    TupleSpec input = result->getInput();
    TupleSpec projection = result->getProjection();


    if (findNode == nodeNames.end()) {
        nodeNames.insert(outputName);

        // Instead of console pass it to a string stream:
        stringstream buffer;

        if (!computationType.compare("apply")) {
            shared_ptr<ApplyLambda> newResult = dynamic_pointer_cast<ApplyLambda>(result);
            string lambdaName = newResult->getLambdaToApply();
            processLamdaName(lambdaName);
            buffer << "apply(" << outputName << ", " << getLowerCaseFirstLetter(input.getSetName())
                   << ", " << getLowerCaseFirstLetter(projection.getSetName()) << ", " << lambdaName
                   << ")." << endl;
        } else if (!computationType.compare("filter")) {
            shared_ptr<ApplyFilter> newResult = dynamic_pointer_cast<ApplyFilter>(result);
            buffer << "filter(" << outputName << ", " << getLowerCaseFirstLetter(input.getSetName())
                   << ", " << getLowerCaseFirstLetter(projection.getSetName()) << ")." << endl;
        } else if (!computationType.compare("hashleft")) {
            shared_ptr<HashLeft> newResult = dynamic_pointer_cast<HashLeft>(result);
            string lambdaName = newResult->getLambdaToApply();
            processLamdaName(lambdaName);
            buffer << "hashleft(" << outputName << ", "
                   << getLowerCaseFirstLetter(input.getSetName()) << ", "
                   << getLowerCaseFirstLetter(projection.getSetName()) << ", " << lambdaName << ")."
                   << endl;
        } else if (!computationType.compare("hashright")) {
            shared_ptr<HashRight> newResult = dynamic_pointer_cast<HashRight>(result);
            string lambdaName = newResult->getLambdaToApply();
            processLamdaName(lambdaName);
            buffer << "hashright(" << outputName << ", "
                   << getLowerCaseFirstLetter(input.getSetName()) << ", "
                   << getLowerCaseFirstLetter(projection.getSetName()) << ", " << lambdaName << ")."
                   << endl;
        } else if (!computationType.compare("joinsets")) {
            shared_ptr<ApplyJoin> newResult = dynamic_pointer_cast<ApplyJoin>(result);
            TupleSpec rInput = newResult->getRightInput();
            TupleSpec rProjection = newResult->getRightProjection();
            buffer << "join(" << outputName << ", " << getLowerCaseFirstLetter(input.getSetName())
                   << ", " << getLowerCaseFirstLetter(projection.getSetName()) << ", "
                   << getLowerCaseFirstLetter(rInput.getSetName()) << ", "
                   << getLowerCaseFirstLetter(rProjection.getSetName()) << ")." << endl;
        }
        // New operations added: AGGREGATE, OUTPUT, FLATTEN, HASHONE.
        // I guess AGGREGATE has no explicit projection.
        else if (!computationType.compare("aggregate")) {
            shared_ptr<ApplyAgg> newResult = dynamic_pointer_cast<ApplyAgg>(result);
            buffer << "aggregate(" << outputName << ", "
                   << getLowerCaseFirstLetter(input.getSetName()) << ")."
                   << endl;  // << ", " << getLowerCaseFirstLetter(projection.getSetName())
        } else if (!computationType.compare("hashone")) {
            shared_ptr<HashOne> newResult = dynamic_pointer_cast<HashOne>(result);
            buffer << "hashone(" << outputName << ", "
                   << getLowerCaseFirstLetter(input.getSetName()) << ", "
                   << getLowerCaseFirstLetter(projection.getSetName()) << ")." << endl;
        } else if (!computationType.compare("flatten")) {
            shared_ptr<Flatten> newResult = dynamic_pointer_cast<Flatten>(result);
            buffer << "flatten(" << outputName << ", "
                   << getLowerCaseFirstLetter(input.getSetName()) << ", "
                   << getLowerCaseFirstLetter(projection.getSetName()) << ")." << endl;
        }
        // I guess OUTPUT has no explicit projection.
        else if (!computationType.compare("writeset")) {
            shared_ptr<WriteSet> newResult = dynamic_pointer_cast<WriteSet>(result);

            // Get database name:
            string dBName = newResult->getDBName();
            makePrologString(dBName);

            // Get set name:
            string setName = newResult->getSetName();
            makePrologString(setName);

            // Create and save the prolog rule:
            buffer << "output(" << outputName << ", " << getLowerCaseFirstLetter(input.getSetName())
                   << ", " << dBName << ", " << setName << ")."
                   << endl;  // << ", " << getLowerCaseFirstLetter(projection.getSetName())
        }

        // If we encounter some unknown operation, abort.
        else {
            cout << computationType << " is not supported currently!" << endl;
            exit(1);
        }

        // Extract the prolog rule:
        string prologRule = buffer.str();

        // Save the created rule to print it later:
        prologRules.push_back(prologRule);
    }


    //*/


    // Follow the links upwards:
    outputName = result->getOutputName();
    vector<AtomicComputationPtr> consumingAtomicComputations =
        acl->getConsumingAtomicComputations(outputName);

    if (consumingAtomicComputations.size()) {
        for (int j = 0; j < consumingAtomicComputations.size(); j++) {
            parseAtomicComputation(acl, consumingAtomicComputations.at(j), outputName, result);
        }
    }

    // When we reach the root node:
    else {

        string root = "virtualRootNode";
        string linkName = root + ", " + outputName;
        // Print a link only if has not been printed before:
        unordered_set<string>::const_iterator findLink = linkNames.find(linkName);
        if (findLink == linkNames.end()) {
            linkNames.insert(linkName);
            stringstream buffer;
            buffer << "link("
                   << "virtualRootNode"
                   << ", " << outputName << ", ";
            // Prepare output list:
            outputAtts = result->getOutput().getAtts();
            buffer << "[";
            for (int i = 0; i < outputAtts.size(); i++) {
                toLowerCaseFirstLetter(outputAtts.at(i));
                buffer << outputAtts.at(i);
                if (i < (outputAtts.size() - 1)) {
                    buffer << ", ";
                }
            }
            buffer << "], ";
            buffer << "[], [])." << endl;

            // Extract the prolog rule:
            string prologRule = buffer.str();

            // Save the created rule to print it later:
            prologRules.push_back(prologRule);
        }
    }
}


// Get the leaves of the computation and go up from there:
void parseAtomicComputationList(AtomicComputationList* result) {

    // Get all the computation leaves:
    vector<AtomicComputationPtr> allScanSets = result->getAllScanSets();

    // Iterate through the leaves and go upwards:
    for (int i = 0; i < allScanSets.size(); i++) {

        // 1) Create the prolog rule "node":
        string computationType = allScanSets.at(i)->getAtomicComputationType();
        toLowerCase(computationType);

        string outputName = allScanSets.at(i)->getOutputName();
        toLowerCaseFirstLetter(outputName);

        string computationName = allScanSets.at(i)->getComputationName();
        makePrologString(computationName);


        // Only print node rules if not previously done:
        unordered_set<string>::const_iterator findNode = nodeNames.find(outputName);
        if (findNode == nodeNames.end()) {
            nodeNames.insert(outputName);

            // Instead of console pass it to a string stream:
            stringstream buffer;
            buffer << "node(" << outputName << ", " << computationType << ", " << computationName
                   << ")." << endl;

            // Extract the prolog rule:
            string prologRule = buffer.str();

            // Save the created rule to print it later:
            prologRules.push_back(prologRule);
        }


        // 2) Create the prolog rule "scan":
        if (!computationType.compare("scan")) {
            shared_ptr<ScanSet> newResult = dynamic_pointer_cast<ScanSet>(allScanSets.at(i));

            // Get database name:
            string dBName = newResult->getDBName();
            makePrologString(dBName);

            // Get set name:
            string setName = newResult->getSetName();
            makePrologString(setName);

            // Create and save the prolog rule:
            stringstream buffer;
            buffer << "scan(" << outputName << ", " << dBName << ", " << setName << ")." << endl;
            string prologRule = buffer.str();
            prologRules.push_back(prologRule);
        }
        // Else, we have a problem here:
        else {
            cout << "We do not expect anything but \"scan\" right here" << endl;
            exit(1);
        }

        // Follow the links upwards:
        outputName = allScanSets.at(i)->getOutputName();
        vector<AtomicComputationPtr> consumingAtomicComputations =
            result->getConsumingAtomicComputations(outputName);
        for (int j = 0; j < consumingAtomicComputations.size(); j++) {
            parseAtomicComputation(
                result, consumingAtomicComputations.at(j), outputName, allScanSets.at(i));
        }
    }
}


// Parse the TCAP string here:
void parseTCAPtoProlog(string& logicalPlan) {
    // The computation list after parsing:
    AtomicComputationList* result;

    // Lexer and Parser:
    yyscan_t scanner;
    LexerExtra extra{""};
    yylex_init_extra(&extra, &scanner);
    const YY_BUFFER_STATE buffer{yy_scan_string(logicalPlan.data(), scanner)};
    const int parseFailed{yyparse(scanner, &result)};
    yy_delete_buffer(buffer, scanner);
    yylex_destroy(scanner);

    if (parseFailed) {
        cout << "Parse error when compiling TCAP: " << extra.errorMessage;
        exit(1);
    }

    parseAtomicComputationList(result);
}


int main() {

    // TCAP String:
    //*
    std::string logicalPlan =
        "inputData (in) <= SCAN ('mySet', 'myData', 'ScanUserSet_0') \n\
	inputWithAtt (in, att) <= APPLY (inputData (in), inputData (in), 'SelectionComp_1', 'methodCall_0') \n\
	inputWithAttAndMethod (in, att, method) <= APPLY (inputWithAtt (in), inputWithAtt (in, att), 'SelectionComp_1', 'attAccess_1') \n\
	inputWithBool (in, bool) <= APPLY (inputWithAttAndMethod (att, method), inputWithAttAndMethod (in), 'SelectionComp_1', '==_2') \n\
	filteredInput (in) <= FILTER (inputWithBool (bool), inputWithBool (in), 'SelectionComp_1') \n\
	projectedInputWithPtr (out) <= APPLY (filteredInput (in), filteredInput (), 'SelectionComp_1', 'methodCall_3') \n\
	projectedInput (out) <= APPLY (projectedInputWithPtr (out), projectedInputWithPtr (), 'SelectionComp_1', 'deref_4') \n\
	aggWithKeyWithPtr (out, key) <= APPLY (projectedInput (out), projectedInput (out), 'AggregationComp_2', 'attAccess_0') \n\
	aggWithKey (out, key) <= APPLY (aggWithKeyWithPtr (key), aggWithKeyWithPtr (out), 'AggregationComp_2', 'deref_1') \n\
	aggWithValue (key, value) <= APPLY (aggWithKey (out), aggWithKey (key), 'AggregationComp_2', 'methodCall_2') \n\
	agg (aggOut) <=	AGGREGATE (aggWithValue (key, value), 'AggregationComp_2') \n\
	checkSales (aggOut, isSales) <= APPLY (agg (aggOut), agg (aggOut), 'SelectionComp_3', 'methodCall_0') \n\
	justSales (aggOut, isSales) <= FILTER (checkSales (isSales), checkSales (aggOut), 'SelectionComp_3') \n\
	final (result) <= APPLY (justSales (aggOut), justSales (), 'SelectionComp_3', 'methodCall_1') \n\
	nothing () <= OUTPUT (final (result), 'outSet', 'myDB', 'SetWriter_4')";
    logicalPlan.push_back('\0');
    //*/

    /*
        std :: string logicalPlan =
        "inputDataForScanUserSet_0(in0) <= SCAN ('tpch_bench_set1', 'TCAP_db', 'ScanUserSet_0')	\n\
        inputDataForScanUserSet_1(in1) <= SCAN ('tpch_bench_set2', 'TCAP_db', 'ScanUserSet_1')	\n\
        attAccess_0ExtractedForJoinComp2(in0,att_ExtractedFor_name) <= APPLY
    (inputDataForScanUserSet_0(in0), inputDataForScanUserSet_0(in0), 'JoinComp_2', 'attAccess_0')
    \n\
        attAccess_0ExtractedForJoinComp2_hashed(in0,att_ExtractedFor_name_hash) <= HASHLEFT
    (attAccess_0ExtractedForJoinComp2(att_ExtractedFor_name), attAccess_0ExtractedForJoinComp2(in0),
    'JoinComp_2', '==_2')	\n\
        attAccess_1ExtractedForJoinComp2(in1,att_ExtractedFor_starName) <= APPLY
    (inputDataForScanUserSet_1(in1), inputDataForScanUserSet_1(in1), 'JoinComp_2', 'attAccess_1')
    \n\
        attAccess_1ExtractedForJoinComp2_hashed(in1,att_ExtractedFor_starName_hash) <= HASHRIGHT
    (attAccess_1ExtractedForJoinComp2(att_ExtractedFor_starName),
    attAccess_1ExtractedForJoinComp2(in1), 'JoinComp_2', '==_2')	\n\
        JoinedFor_equals2JoinComp2(in0, in1) <= JOIN
    (attAccess_0ExtractedForJoinComp2_hashed(att_ExtractedFor_name_hash),
    attAccess_0ExtractedForJoinComp2_hashed(in0),
    attAccess_1ExtractedForJoinComp2_hashed(att_ExtractedFor_starName_hash),
    attAccess_1ExtractedForJoinComp2_hashed(in1), 'JoinComp_2') \n\
        JoinedFor_equals2JoinComp2_WithLHSExtracted(in0,in1,LHSExtractedFor_2_2) <= APPLY
    (JoinedFor_equals2JoinComp2(in0), JoinedFor_equals2JoinComp2(in0,in1), 'JoinComp_2',
    'attAccess_0')	\n\
        JoinedFor_equals2JoinComp2_WithBOTHExtracted(in0,in1,LHSExtractedFor_2_2,RHSExtractedFor_2_2)
    <= APPLY (JoinedFor_equals2JoinComp2_WithLHSExtracted(in1),
    JoinedFor_equals2JoinComp2_WithLHSExtracted(in0,in1,LHSExtractedFor_2_2), 'JoinComp_2',
    'attAccess_1')	\n\
        JoinedFor_equals2JoinComp2_BOOL(in0,in1,bool_2_2) <= APPLY
    (JoinedFor_equals2JoinComp2_WithBOTHExtracted(LHSExtractedFor_2_2,RHSExtractedFor_2_2),
    JoinedFor_equals2JoinComp2_WithBOTHExtracted(in0,in1), 'JoinComp_2', '==_2')	\n\
        JoinedFor_equals2JoinComp2_FILTERED(in0, in1) <= FILTER
    (JoinedFor_equals2JoinComp2_BOOL(bool_2_2), JoinedFor_equals2JoinComp2_BOOL(in0, in1),
    'JoinComp_2')	\n\
        nativ_3OutForJoinComp2 (nativ_3_2OutFor) <= APPLY (JoinedFor_equals2JoinComp2_FILTERED(in0),
    JoinedFor_equals2JoinComp2_FILTERED(), 'JoinComp_2', 'native_lambda_3')	\n\
        attAccess_0OutForSelectionComp3(nativ_3_2OutFor,att_OutFor_birthYear) <= APPLY
    (nativ_3OutForJoinComp2(nativ_3_2OutFor), nativ_3OutForJoinComp2(nativ_3_2OutFor),
    'SelectionComp_3', 'attAccess_0')	\n\
        attAccess_1OutForSelectionComp3(nativ_3_2OutFor,att_OutFor_birthYear,att_OutFor_checkBirthYear)
    <= APPLY (attAccess_0OutForSelectionComp3(nativ_3_2OutFor),
    attAccess_0OutForSelectionComp3(nativ_3_2OutFor,att_OutFor_birthYear), 'SelectionComp_3',
    'attAccess_1')	\n\
        equals_2OutForSelectionComp3(nativ_3_2OutFor,att_OutFor_birthYear,att_OutFor_checkBirthYear,bool_2_3)
    <= APPLY (attAccess_1OutForSelectionComp3(att_OutFor_birthYear,att_OutFor_checkBirthYear),
    attAccess_1OutForSelectionComp3(nativ_3_2OutFor,att_OutFor_birthYear,att_OutFor_checkBirthYear),
    'SelectionComp_3', '==_2')	\n\
        filteredInputForSelectionComp3(nativ_3_2OutFor) <= FILTER
    (equals_2OutForSelectionComp3(bool_2_3), equals_2OutForSelectionComp3(nativ_3_2OutFor),
    'SelectionComp_3')	\n\
        nativ_3OutForSelectionComp3 (nativ_3_3OutFor) <= APPLY
    (filteredInputForSelectionComp3(nativ_3_2OutFor), filteredInputForSelectionComp3(),
    'SelectionComp_3', 'native_lambda_3')";
        logicalPlan.push_back ('\0');
    //*/


    parseTCAPtoProlog(logicalPlan);

    //*
    // Sort the rules and print on console:
    sort(prologRules.begin(), prologRules.end());
    for (int i = 0; i < prologRules.size(); i++) {
        cout << prologRules.at(i);
    }
    //*/
}


#endif