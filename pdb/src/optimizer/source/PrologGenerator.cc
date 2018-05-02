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

#include <iostream>
#include <vector>
#include <sstream>
#include <algorithm>
#include <unordered_set>
#include "PDBDebug.h"
#include "Lexer.h"
#include "Parser.h"
#include "AtomicComputationList.h"
#include "AtomicComputationClasses.h"
#include "PrologGenerator.h"

namespace pdb {

void PrologGenerator::parseTCAP(std::string &logicalPlan) {
  // The computation list after parsing:
  AtomicComputationList *result;

  // Lexer and Parser:
  yyscan_t scanner;
  LexerExtra extra{""};
  yylex_init_extra(&extra, &scanner);
  const YY_BUFFER_STATE buffer{yy_scan_string(logicalPlan.data(), scanner)};
  const int parseFailed{yyparse(scanner, &result)};
  yy_delete_buffer(buffer, scanner);
  yylex_destroy(scanner);

  if (parseFailed) {
    PDB_COUT << "Parse error when compiling TCAP: " << extra.errorMessage;
    exit(1);
  }

  parseAtomicComputationList(result);
}

const std::vector<std::string> &PrologGenerator::getPrologRules() {
  return prologRules;
}

// Get the leaves of the computation and go up from there:
void PrologGenerator::parseAtomicComputationList(AtomicComputationList *result) {

  // Get all the computation leaves:
  std::vector<AtomicComputationPtr> allScanSets = result->getAllScanSets();

  // Iterate through the leaves and go upwards:
  for (auto &scanSet : allScanSets) {

    // 1) Create the prolog rule "node":
    std::string computationType = scanSet->getAtomicComputationType();
    toLowerCase(computationType);

    std::string outputName = getLowerCaseFirstLetter(scanSet->getOutputName());

    std::string computationName = makePrologString(scanSet->getComputationName());

    // Only print node rules if not previously done:
    std::unordered_set<std::string>::const_iterator findNode = nodeNames.find(outputName);
    if (findNode == nodeNames.end()) {
      nodeNames.insert(outputName);

      auto info = mapToString(*scanSet->getKeyValuePairs());

      // Instead of console pass it to a string stream:
      std::stringstream buffer;
      buffer << "node(" << outputName << ", " << toLowerCase(scanSet->getAtomicComputationType()) << ", " << computationName << ", " << info << ")." << std::endl;

      // Extract the prolog rule:
      std::string prologRule = buffer.str();

      // Save the created rule to print it later:
      prologRules.push_back(prologRule);
    }

    // 2) Create the prolog rule "scan":
    if (scanSet->getAtomicComputationTypeID() == ScanSetAtomicTypeID) {
      std::shared_ptr<ScanSet> newResult = std::dynamic_pointer_cast<ScanSet>(scanSet);

      // Get database name:
      std::string dBName = makePrologString(newResult->getDBName());

      // Get set name:
      std::string setName = makePrologString(newResult->getSetName());

      // Create and save the prolog rule:
      std::stringstream buffer;
      buffer << "scan(" << outputName << ", " << dBName << ", " << setName << ")." << std::endl;
      std::string prologRule = buffer.str();
      prologRules.push_back(prologRule);
    }
    // Else, we have a problem here:
    else {
      PDB_COUT << "We do not expect anything but \"scan\" right here\n";
      exit(1);
    }

    // Follow the links upwards:
    outputName = scanSet->getOutputName();
    std::vector<AtomicComputationPtr> consumingAtomicComputations = result->getConsumingAtomicComputations(outputName);
    for (const auto &consumingAtomicComputation : consumingAtomicComputations) {
      parseAtomicComputation(result, consumingAtomicComputation, outputName, scanSet);
    }
  }
}

void PrologGenerator::parseAtomicComputation(AtomicComputationList* acl,
                                             AtomicComputationPtr result,
                                             std::string& inputName,
                                             AtomicComputationPtr oldResult) {

  // Change "inputName" to lower case:
  inputName = getLowerCaseFirstLetter(inputName);

  // 1) Create the prolog rule "node":
  std::string outputName = result->getOutputName();
  outputName = getLowerCaseFirstLetter(outputName);

  std::string computationName = makePrologString(result->getComputationName());

  // Only print node rules if not previously done:
  std::unordered_set<std::string>::const_iterator findNode = nodeNames.find(outputName);
  if (findNode == nodeNames.end()) {
    // nodeNames.insert(outputName);

    auto info = mapToString(*result->getKeyValuePairs());

    // Instead of console pass it to a string stream:
    std::stringstream buffer;
    if (result->getAtomicComputationTypeID() == ApplyJoinTypeID) {
      buffer << "node(" << outputName << ", " << "join" << ", " << computationName << ", " << info << ")." << std::endl;
    } else if (result->getAtomicComputationTypeID() == WriteSetTypeID) {
      buffer << "node(" << outputName << ", " << "output" << ", " << computationName << ", " << info << ")." << std::endl;
    } else {
      buffer << "node(" << outputName << ", " << toLowerCase(result->getAtomicComputationType()) << ", " << computationName << ", " << info << ")." << std::endl;
    }

    // Extract the prolog rule:
    std::string prologRule = buffer.str();

    // Save the created rule to print it later:
    prologRules.push_back(prologRule);
  }

  //*
  // 2) Create the prolog rule "link":
  std::string linkName = outputName + ", " + inputName;

  // Get required attributes (output, input and projection_:
  std::vector<std::string> outputAtts;
  std::vector<std::string> inputAtts;
  std::vector<std::string> projectionAtts;


  // Print a link only if has not been printed before:
  std::unordered_set<std::string>::const_iterator findLink = linkNames.find(linkName);
  if (findLink == linkNames.end()) {
    linkNames.insert(linkName);

    // Instead of console pass it to a string stream:
    std::stringstream buffer;

    buffer << "link(" << outputName << ", " << inputName << ", ";

    // Prepare output list:
    outputAtts = oldResult->getOutput().getAtts();
    buffer << "[";
    for (unsigned long i = 0; i < outputAtts.size(); i++) {
      buffer << getLowerCaseFirstLetter(outputAtts.at(i));
      if (i < (outputAtts.size() - 1)) {
        buffer << ", ";
      }
    }
    buffer << "], ";


    // Join:
    if (result->getAtomicComputationTypeID() == ApplyJoinTypeID) {
      std::string sourceComputationType = oldResult->getAtomicComputationType();
      sourceComputationType = toLowerCase(sourceComputationType);

      if (oldResult->getAtomicComputationTypeID() == HashRightTypeID) {
        std::shared_ptr<ApplyJoin> newResult = std::dynamic_pointer_cast<ApplyJoin>(result);
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
    for (unsigned i = 0; i < inputAtts.size(); i++) {
      buffer << getLowerCaseFirstLetter(inputAtts.at(i));
      if (i < (inputAtts.size() - 1)) {
        buffer << ", ";
      }
    }
    buffer << "], ";


    // Prepare projection list:
    buffer << "[";
    for (unsigned long i = 0; i < projectionAtts.size(); i++) {
      buffer << getLowerCaseFirstLetter(projectionAtts.at(i));
      if (i < (projectionAtts.size() - 1)) {
        buffer << ", ";
      }
    }
    buffer << "]";
    buffer << ")." << std::endl;


    // Extract the prolog rule:
    std::string prologRule = buffer.str();

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
    std::stringstream buffer;


    switch (result->getAtomicComputationTypeID()) {
      case ApplyLambdaTypeID: {
        std::shared_ptr<ApplyLambda> newResult = std::dynamic_pointer_cast<ApplyLambda>(result);
        std::string lambdaName = processLambdaName(newResult->getLambdaToApply());
        buffer << "apply(" << outputName << ", " << getLowerCaseFirstLetter(input.getSetName())
               << ", " << getLowerCaseFirstLetter(projection.getSetName()) << ", " << lambdaName
               << ")." << std::endl;
        break;
      }
      case ApplyFilterTypeID: {
        std::shared_ptr<ApplyFilter> newResult = std::dynamic_pointer_cast<ApplyFilter>(result);
        buffer << "filter(" << outputName << ", " << getLowerCaseFirstLetter(input.getSetName())
               << ", " << getLowerCaseFirstLetter(projection.getSetName()) << ")." << std::endl;
        break;
      }
      case HashLeftTypeID: {
        std::shared_ptr<HashLeft> newResult = std::dynamic_pointer_cast<HashLeft>(result);
        std::string lambdaName = processLambdaName(newResult->getLambdaToApply());
        buffer << "hashleft(" << outputName << ", "
               << getLowerCaseFirstLetter(input.getSetName()) << ", "
               << getLowerCaseFirstLetter(projection.getSetName()) << ", " << lambdaName << ")."
               << std::endl;
        break;
      }
      case HashRightTypeID: {
        std::shared_ptr<HashRight> newResult = std::dynamic_pointer_cast<HashRight>(result);
        std::string lambdaName = processLambdaName(newResult->getLambdaToApply());
        buffer << "hashright(" << outputName << ", "
               << getLowerCaseFirstLetter(input.getSetName()) << ", "
               << getLowerCaseFirstLetter(projection.getSetName()) << ", " << lambdaName << ")."
               << std::endl;
        break;
      }
      case ApplyJoinTypeID: {
        std::shared_ptr<ApplyJoin> newResult = std::dynamic_pointer_cast<ApplyJoin>(result);
        TupleSpec rInput = newResult->getRightInput();
        TupleSpec rProjection = newResult->getRightProjection();
        buffer << "join(" << outputName << ", " << getLowerCaseFirstLetter(input.getSetName())
               << ", " << getLowerCaseFirstLetter(projection.getSetName()) << ", "
               << getLowerCaseFirstLetter(rInput.getSetName()) << ", "
               << getLowerCaseFirstLetter(rProjection.getSetName()) << ")." << std::endl;
        break;
      }
      case ApplyAggTypeID: {
        std::shared_ptr<ApplyAgg> newResult = std::dynamic_pointer_cast<ApplyAgg>(result);
        buffer << "aggregate(" << outputName << ", "
               << getLowerCaseFirstLetter(input.getSetName()) << ")."
               << std::endl;  // << ", " << getLowerCaseFirstLetter(projection.getSetName())
        break;
      }
      case HashOneTypeID: {
        std::shared_ptr<HashOne> newResult = std::dynamic_pointer_cast<HashOne>(result);
        buffer << "hashone(" << outputName << ", "
               << getLowerCaseFirstLetter(input.getSetName()) << ", "
               << getLowerCaseFirstLetter(projection.getSetName()) << ")." << std::endl;
        break;
      }
      case FlattenTypeID: {
        std::shared_ptr<Flatten> newResult = std::dynamic_pointer_cast<Flatten>(result);
        buffer << "flatten(" << outputName << ", "
               << getLowerCaseFirstLetter(input.getSetName()) << ", "
               << getLowerCaseFirstLetter(projection.getSetName()) << ")." << std::endl;
        break;
      }
      case WriteSetTypeID: {
        std::shared_ptr<WriteSet> newResult = std::dynamic_pointer_cast<WriteSet>(result);

        // Get database name:
        std::string dBName = makePrologString(newResult->getDBName());

        // Get set name:
        std::string setName = makePrologString(newResult->getSetName());

        // Create and save the prolog rule:
        buffer << "output(" << outputName << ", " << getLowerCaseFirstLetter(input.getSetName())
               << ", " << dBName << ", " << setName << ")."
               << std::endl;  // << ", " << getLowerCaseFirstLetter(projection.getSetName())
        break;
      }
      default: {
        std::cout << result->getAtomicComputationType() << " is not supported currently!" << std::endl;
        exit(1);
      }
    }

    // Extract the prolog rule:
    std::string prologRule = buffer.str();

    // Save the created rule to print it later:
    prologRules.push_back(prologRule);
  }

  // Follow the links upwards:
  outputName = result->getOutputName();
  std::vector<AtomicComputationPtr> consumingAtomicComputations = acl->getConsumingAtomicComputations(outputName);

  if (!consumingAtomicComputations.empty()) {
    for (const auto &consumingAtomicComputation : consumingAtomicComputations) {
      parseAtomicComputation(acl, consumingAtomicComputation, outputName, result);
    }
  }
  // When we reach the root node:
  else {

    std::string root = "virtualRootNode";

    // TODO You declared linkName twice so I renamed this one to linkNameLocal
    std::string linkNameLocal = root + ", " + outputName;
    // Print a link only if has not been printed before:
    // TODO You did exactly the same thing here
    std::unordered_set<std::string>::const_iterator findLinkLocal = linkNames.find(linkNameLocal);
    if (findLinkLocal == linkNames.end()) {
      linkNames.insert(linkNameLocal);
      std::stringstream buffer;
      buffer << "link(" << "virtualRootNode" << ", " << outputName << ", ";
      // Prepare output list:
      outputAtts = result->getOutput().getAtts();
      buffer << "[";
      for (unsigned i = 0; i < outputAtts.size(); i++) {
        buffer << getLowerCaseFirstLetter(outputAtts.at(i));
        if (i < (outputAtts.size() - 1)) {
          buffer << ", ";
        }
      }
      buffer << "], ";
      buffer << "[], [])." << std::endl;

      // Extract the prolog rule:
      std::string prologRule = buffer.str();

      // Save the created rule to print it later:
      prologRules.push_back(prologRule);
    }
  }
}


std::string PrologGenerator::getLowerCaseFirstLetter(std::string data) {
  data[0] = static_cast<char>(tolower(data[0]));
  return data;
}

void PrologGenerator::toLowerCaseFirstLetter(std::string &data) {
  data[0] = static_cast<char>(tolower(data[0]));
}

std::string PrologGenerator::toLowerCase(std::string data) {
  transform(data.begin(), data.end(), data.begin(), ::tolower);
  return data;
}

// In the Prolog rules data would appear as 'data'
std::string PrologGenerator::makePrologString(const std::string &data) {
  return "'" + data + "'";
}

std::string PrologGenerator::processLambdaName(const std::string &data) {
  // data = "lambda_"  + data;
  return makePrologString(data);
}

std::string PrologGenerator::mapToString(std::map<std::string, std::string> &map) {

  // if the map is empty we just return a constant string
  if(map.empty()){
    return "[]";
  }

  // create the string
  std::string info = "[";
  for (const auto &i : map) {
    info += "('" + i.first + "','" + i.second + "'),";
  }

  // this will remove the last comma and add a closing square bracket
  info.pop_back();
  info.push_back(']');

  return info;
}

}