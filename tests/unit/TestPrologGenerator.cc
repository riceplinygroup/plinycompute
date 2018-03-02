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


  std::string logicalPlan = "inputDataForScanUserSet_0(in0) <= SCAN ('chris_set', 'chris_db', 'ScanUserSet_0')\n"
      "\n"
      "                /* Apply selection filtering */\n"
      "                nativ_0OutForSelectionComp1(in0,nativ_0_1OutFor) <= APPLY (inputDataForScanUserSet_0(in0), inputDataForScanUserSet_0(in0), 'SelectionComp_1', 'native_lambda_0')\n"
      "                filteredInputForSelectionComp1(in0) <= FILTER (nativ_0OutForSelectionComp1(nativ_0_1OutFor), nativ_0OutForSelectionComp1(in0), 'SelectionComp_1')\n"
      "\n"
      "                /* Apply selection projection */\n"
      "                nativ_1OutForSelectionComp1 (nativ_1_1OutFor) <= APPLY (filteredInputForSelectionComp1(in0), filteredInputForSelectionComp1(), 'SelectionComp_1', 'native_lambda_1')\n"
      "                nativ_1OutForSelectionComp1_out( ) <= OUTPUT ( nativ_1OutForSelectionComp1 ( nativ_1_1OutFor ), 'output_set1', 'chris_db', 'WriteUserSet_2')";

  PrologOptimizer optimizer;

  std::cout << optimizer.optimize(logicalPlan);

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
