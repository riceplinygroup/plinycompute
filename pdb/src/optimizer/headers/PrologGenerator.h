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

#ifndef PDB_PROLOGGENERATOR_H
#define PDB_PROLOGGENERATOR_H

#include <string>
#include <unordered_set>
#include <vector>
#include "AtomicComputationList.h"

namespace pdb {

class PrologGenerator {

 public:

  /**
   * Takes in a logical plan and generates the prolog rules
   * @param logicalPlan
   */
  void parseTCAP(std::string& logicalPlan);


  /**
   * Returns the rules generated from the parsed prolog
   * @return the rules
   */
  const std::vector<std::string>& getPrologRules();

 private:

  /**
   * This method converts a hashmap to a prolog map in string form
   * @param map the hash map
   * @return the prolog string ['k1'-'v1', 'k2'-'v2', 'k3'-'v3']
   */
  std::string mapToString(std::map<std::string, std::string> &map);

  /**
   * //TODO Sourav describe what it contains
   * @param result
   */
  void parseAtomicComputationList(AtomicComputationList* result);

  /**
   * //TODO Sourav describe what it contains
   */
  std::unordered_set<std::string> nodeNames;

  /**
   * //TODO Sourav describe what it contains
   */
  std::unordered_set<std::string> linkNames;

  /**
   * //TODO Sourav describe what it contains
   */
  std::vector<std::string> prologRules;

  /**
   * //TODO Sourav describe what your method does
   * @param data
   * @return
   */
  std::string getLowerCaseFirstLetter(std::string data);

  /**
   * //TODO Sourav describe what your method does
   * @param data
   */
  void toLowerCaseFirstLetter(std::string& data);

  /**
   * //TODO Sourav describe what your method does
   * @param data
   */
  std::string toLowerCase(std::string data);

  /**
   * //TODO Sourav describe what your method does
   * @param data
   */
  std::string makePrologString(const std::string &data);

  /**
   * //TODO Sourav describe what your method does
   * @param acl
   * @param result
   * @param inputName
   * @param oldResult
   */
  void parseAtomicComputation(AtomicComputationList* acl,
                              AtomicComputationPtr result,
                              std::string& inputName,
                              AtomicComputationPtr oldResult);

  /**
   * //TODO Sourav describe what your method does
   * @param data
   */
  std::string processLambdaName(const std::string &data);
};

}



#endif //PDB_PROLOGGENERATOR_H
