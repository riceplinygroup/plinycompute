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

#include <algorithm>
#include <iostream>
#include <PrologOptimizer.h>
#include "PrologGenerator.h"

int main() {


  std::string logicalPlan = "inputDataForScanUserSet_0(in0) <= SCAN ('test78_set1', 'test78_db', 'ScanUserSet_0')\n"
      "inputDataForScanUserSet_1(in1) <= SCAN ('test78_set2', 'test78_db', 'ScanUserSet_1')\n"
      "\n"
      "/* Apply selection filtering */\n"
      "nativ_0OutForSelectionComp2(in1,nativ_0_2OutFor) <= APPLY (inputDataForScanUserSet_1(in1), inputDataForScanUserSet_1(in1), 'SelectionComp_2', 'native_lambda_0')\n"
      "filteredInputForSelectionComp2(in1) <= FILTER (nativ_0OutForSelectionComp2(nativ_0_2OutFor), nativ_0OutForSelectionComp2(in1), 'SelectionComp_2')\n"
      "\n"
      "/* Apply selection projection */\n"
      "attAccess_1OutForSelectionComp2(in1,att_1OutFor_myString) <= APPLY (filteredInputForSelectionComp2(in1), filteredInputForSelectionComp2(in1), 'SelectionComp_2', 'attAccess_1')\n"
      "deref_2OutForSelectionComp2 (att_1OutFor_myString) <= APPLY (attAccess_1OutForSelectionComp2(att_1OutFor_myString), attAccess_1OutForSelectionComp2(), 'SelectionComp_2', 'deref_2')\n"
      "self_0ExtractedJoinComp3(in0,self_0_3Extracted) <= APPLY (inputDataForScanUserSet_0(in0), inputDataForScanUserSet_0(in0), 'JoinComp_3', 'self_0')\n"
      "self_0ExtractedJoinComp3_hashed(in0,self_0_3Extracted_hash) <= HASHLEFT (self_0ExtractedJoinComp3(self_0_3Extracted), self_0ExtractedJoinComp3(in0), 'JoinComp_3', '==_2')\n"
      "attAccess_1ExtractedForJoinComp3(in1,att_1ExtractedFor_myInt) <= APPLY (inputDataForScanUserSet_1(in1), inputDataForScanUserSet_1(in1), 'JoinComp_3', 'attAccess_1')\n"
      "attAccess_1ExtractedForJoinComp3_hashed(in1,att_1ExtractedFor_myInt_hash) <= HASHRIGHT (attAccess_1ExtractedForJoinComp3(att_1ExtractedFor_myInt), attAccess_1ExtractedForJoinComp3(in1), 'JoinComp_3', '==_2')\n"
      "\n"
      "/* Join ( in0 ) and ( in1 ) */\n"
      "JoinedFor_equals2JoinComp3(in0, in1) <= JOIN (self_0ExtractedJoinComp3_hashed(self_0_3Extracted_hash), self_0ExtractedJoinComp3_hashed(in0), attAccess_1ExtractedForJoinComp3_hashed(att_1ExtractedFor_myInt_hash), attAccess_1ExtractedForJoinComp3_hashed(in1), 'JoinComp_3')\n"
      "JoinedFor_equals2JoinComp3_WithLHSExtracted(in0,in1,LHSExtractedFor_2_3) <= APPLY (JoinedFor_equals2JoinComp3(in0), JoinedFor_equals2JoinComp3(in0,in1), 'JoinComp_3', 'self_0')\n"
      "JoinedFor_equals2JoinComp3_WithBOTHExtracted(in0,in1,LHSExtractedFor_2_3,RHSExtractedFor_2_3) <= APPLY (JoinedFor_equals2JoinComp3_WithLHSExtracted(in1), JoinedFor_equals2JoinComp3_WithLHSExtracted(in0,in1,LHSExtractedFor_2_3), 'JoinComp_3', 'attAccess_1')\n"
      "JoinedFor_equals2JoinComp3_BOOL(in0,in1,bool_2_3) <= APPLY (JoinedFor_equals2JoinComp3_WithBOTHExtracted(LHSExtractedFor_2_3,RHSExtractedFor_2_3), JoinedFor_equals2JoinComp3_WithBOTHExtracted(in0,in1), 'JoinComp_3', '==_2')\n"
      "JoinedFor_equals2JoinComp3_FILTERED(in0, in1) <= FILTER (JoinedFor_equals2JoinComp3_BOOL(bool_2_3), JoinedFor_equals2JoinComp3_BOOL(in0, in1), 'JoinComp_3')\n"
      "attAccess_3ExtractedForJoinComp3(in0,in1,att_3ExtractedFor_myString) <= APPLY (JoinedFor_equals2JoinComp3_FILTERED(in1), JoinedFor_equals2JoinComp3_FILTERED(in0,in1), 'JoinComp_3', 'attAccess_3')\n"
      "attAccess_3ExtractedForJoinComp3_hashed(in0,in1,att_3ExtractedFor_myString_hash) <= HASHLEFT (attAccess_3ExtractedForJoinComp3(att_3ExtractedFor_myString), attAccess_3ExtractedForJoinComp3(in0,in1), 'JoinComp_3', '==_5')\n"
      "self_4ExtractedJoinComp3(att_1OutFor_myString,self_4_3Extracted) <= APPLY (deref_2OutForSelectionComp2(att_1OutFor_myString), deref_2OutForSelectionComp2(att_1OutFor_myString), 'JoinComp_3', 'self_4')\n"
      "self_4ExtractedJoinComp3_hashed(att_1OutFor_myString,self_4_3Extracted_hash) <= HASHRIGHT (self_4ExtractedJoinComp3(self_4_3Extracted), self_4ExtractedJoinComp3(att_1OutFor_myString), 'JoinComp_3', '==_5')\n"
      "\n"
      "/* Join ( in0 in1 ) and ( att_1OutFor_myString ) */\n"
      "JoinedFor_equals5JoinComp3(in0, in1, att_1OutFor_myString) <= JOIN (attAccess_3ExtractedForJoinComp3_hashed(att_3ExtractedFor_myString_hash), attAccess_3ExtractedForJoinComp3_hashed(in0, in1), self_4ExtractedJoinComp3_hashed(self_4_3Extracted_hash), self_4ExtractedJoinComp3_hashed(att_1OutFor_myString), 'JoinComp_3')\n"
      "JoinedFor_equals5JoinComp3_WithLHSExtracted(in0,in1,att_1OutFor_myString,LHSExtractedFor_5_3) <= APPLY (JoinedFor_equals5JoinComp3(in1), JoinedFor_equals5JoinComp3(in0,in1,att_1OutFor_myString), 'JoinComp_3', 'attAccess_3')\n"
      "JoinedFor_equals5JoinComp3_WithBOTHExtracted(in0,in1,att_1OutFor_myString,LHSExtractedFor_5_3,RHSExtractedFor_5_3) <= APPLY (JoinedFor_equals5JoinComp3_WithLHSExtracted(att_1OutFor_myString), JoinedFor_equals5JoinComp3_WithLHSExtracted(in0,in1,att_1OutFor_myString,LHSExtractedFor_5_3), 'JoinComp_3', 'self_4')\n"
      "JoinedFor_equals5JoinComp3_BOOL(in0,in1,att_1OutFor_myString,bool_5_3) <= APPLY (JoinedFor_equals5JoinComp3_WithBOTHExtracted(LHSExtractedFor_5_3,RHSExtractedFor_5_3), JoinedFor_equals5JoinComp3_WithBOTHExtracted(in0,in1,att_1OutFor_myString), 'JoinComp_3', '==_5')\n"
      "JoinedFor_equals5JoinComp3_FILTERED(in0, in1, att_1OutFor_myString) <= FILTER (JoinedFor_equals5JoinComp3_BOOL(bool_5_3), JoinedFor_equals5JoinComp3_BOOL(in0, in1, att_1OutFor_myString), 'JoinComp_3')\n"
      "\n"
      "/* run Join projection on ( in0 )*/\n"
      "nativ_7OutForJoinComp3 (nativ_7_3OutFor) <= APPLY (JoinedFor_equals5JoinComp3_FILTERED(in0), JoinedFor_equals5JoinComp3_FILTERED(), 'JoinComp_3', 'native_lambda_7')\n"
      "\n"
      "/* Extract key for aggregation */\n"
      "nativ_0OutForClusterAggregationComp4(nativ_7_3OutFor,nativ_0_4OutFor) <= APPLY (nativ_7OutForJoinComp3(nativ_7_3OutFor), nativ_7OutForJoinComp3(nativ_7_3OutFor), 'ClusterAggregationComp_4', 'native_lambda_0')\n"
      "\n"
      "/* Extract value for aggregation */\n"
      "nativ_1OutForClusterAggregationComp4(nativ_0_4OutFor,nativ_1_4OutFor) <= APPLY (nativ_0OutForClusterAggregationComp4(nativ_7_3OutFor), nativ_0OutForClusterAggregationComp4(nativ_0_4OutFor), 'ClusterAggregationComp_4', 'native_lambda_1')\n"
      "\n"
      "/* Apply aggregation */\n"
      "aggOutForClusterAggregationComp4 (aggOutFor4)<= AGGREGATE (nativ_1OutForClusterAggregationComp4(nativ_0_4OutFor, nativ_1_4OutFor),'ClusterAggregationComp_4')\n"
      "out( ) <= OUTPUT ( aggOutForClusterAggregationComp4 ( aggOutFor4 ), 'output_set1', 'test78_db', 'WriteUserSet_5')";

  PrologOptimizer optimizer;

  std::cout << "\033[1;31m" << optimizer.optimize(logicalPlan) << "\033[0m";

//  // add the end character
//  logicalPlan.push_back('\0');
//
//  // create the prolog generator
//  pdb::PrologGenerator planGenerator;
//
//  // parse the TCAP
//  planGenerator.parseTCAP(logicalPlan);
//
//  // grab the prolog rules
//  auto prologRules = planGenerator.getPrologRules();
//
//  //*
//  // Sort the rules and print on console:
//  std::sort(prologRules.begin(), prologRules.end());
//  for (const auto &prologRule : prologRules) {
//    std::cout << prologRule;
//  }
}
