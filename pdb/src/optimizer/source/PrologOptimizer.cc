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

#include <fstream>
#include <SWI-cpp.h>
#include <sstream>
#include <algorithm>
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

  // create the arguments to initialize the prolog engine
  std::vector<std::string> prologArguments = { "swipl", "-f", storageLocation + "PrologToTCAP.pl", "-s", storageLocation + "input.pl"};

  // converts our array of strings to an array of char* so we can pass it in the prolog engine
  std::vector<char *> argv = toArgv(prologArguments);

  // initialize the prolog engine
  PlEngine engine((int)prologArguments.size(), argv.data());

  // prepare the query to run the optimizer
  PlTermv emptyTerm(0);
  PlQuery optimizeQuery("tcapGenerator", emptyTerm);

  // check if we succeeded in optimizing the query
  if(!optimizeQuery.next_solution()){
    std::cout<< "Failed to perform optimization. Will use the unoptimized string" << std::endl;
    return tcapString;
  }

  // issue the query to get the file name of the optimized query
  PlTermv getFileName(1);
  PlQuery getFileQuery("getFile", getFileName);

  // check if we could get the filename
  if(!getFileQuery.next_solution()){
    std::cout<< "Failed to perform optimization. Will use the unoptimized string" << std::endl;
    return tcapString;
  }

  // grab the filename
  std::string fileName((char*) getFileName[0]);

  // create a buffer to store the optimized tcap string
  std::ostringstream optimizedString( std::ios::out | std::ios::binary ) ;

  // open the file with the tcap string and read it in
  std::ifstream inFile(fileName.c_str()) ;
  std::string line;
  while(std::getline(inFile, line)) {
    optimizedString << line << "\r\n";
  }

  // return the optimized string
  return optimizedString.str();
}
std::vector<char *> PrologOptimizer::toArgv(const std::vector<std::string> &prologArguments) const {
  std::vector<char*> argv;
  for (const auto& arg : prologArguments) {
    argv.push_back((char*)arg.data());
  }
  argv.push_back(nullptr);
  return argv;
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