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

#include <boost/filesystem/operations.hpp>
#include <fstream>
#include <sstream>
#include <mustache.hpp>
#include "PrologGenerator.h"
#include "PrologOptimizer.h"

std::string PrologOptimizer::optimize(std::string tcapString) {

  // create the prolog generator
  pdb::PrologGenerator planGenerator;

  // parse the TCAP
  planGenerator.parseTCAP(tcapString);

  // grab the prolog rules
  auto rules = planGenerator.getPrologRules();

  // write out the prolog rules to a file
  writePrologRules(rules);

  // write out the prolog to tcap script
  writePrologToTCAPScript();

  // get the prolog command
  std::string command = getPrologCommand();

  // run the command
  system(command.c_str());

  // set the output file name
  std::string outputFileName(storageLocation + "tcapOutput.tcap");

  // create a buffer to store the optimized tcap string
  std::ostringstream optimizedString( std::ios::out | std::ios::binary ) ;

  // open the file with the tcap string and read it in
  std::ifstream inFile(outputFileName.c_str()) ;
  std::string line;
  while(std::getline(inFile, line)) {
    optimizedString << line << "\r\n";
  }

  // remove the file we just created
  boost::filesystem::remove(outputFileName);

  // return the optimized string
  return optimizedString.str();
}

std::string PrologOptimizer::getPrologCommand() const {

  // command template
  mustache::mustache commandTemplate{"swipl -f {{prologToTcap}} -s {{inputFile}} -g \"{{command}}\""};

  // create the command data
  mustache::data commandData;
  commandData.set("prologToTcap", storageLocation + "PrologToTCAP.pl");
  commandData.set("inputFile", storageLocation + "input.pl");
  commandData.set("command", "tcapGenerator,halt");

  return commandTemplate.render(commandData);
}

void PrologOptimizer::writePrologToTCAPScript() const {

  // open the PrologToTCAP.pl for writing
  std::ofstream prologToTCAP(storageLocation + "PrologToTCAP.pl", std::ios_base::out);

  // write out the script
  prologToTCAP << prologToTCAPScript;

  // close the file
  prologToTCAP.close();
}

void PrologOptimizer::writePrologRules(std::vector<std::string> &rules) const {

  // open a file to store the rules
  std::ofstream rulesOutputFile;
  rulesOutputFile.open(storageLocation + "input.pl", std::ios_base::out);

  // sort the rules
  std::sort(rules.begin(), rules.end());

  // write them out to the file
  for(auto &rule : rules) {
    rulesOutputFile << rule;
  }

  // close the file
  rulesOutputFile.close();
}

const std::string PrologOptimizer::storageLocation = "/tmp/";