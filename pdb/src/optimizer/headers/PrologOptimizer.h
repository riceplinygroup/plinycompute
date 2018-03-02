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

#ifndef PDB_PROLOGOPTIMIZER_H
#define PDB_PROLOGOPTIMIZER_H

#include <string>
#include <vector>
#include "TCAPOptimizer.h"


class PrologOptimizer : public TCAPOptimizer {

public:

  /**
   * This method is going to do the following stuff :
   * 1. Generate the prolog rules using the PrologGenerator class
   * 2. Write these rules out to a file
   * 3. Write the optimizer to a file
   * 4. Run the optimizer
   * 5. Get the optimized file
   * 6. Remove it
   * 7. Return the optimized tcap string
   * @param tcapString the optimized string
   */
  std::string optimize(std::string tcapString) override;

private:

  /**
   * This is where we are going to store the prolog files and the optimizer files
   */
  const static std::string storageLocation;

  /**
   * This string contains the whole PrologToTCAP.pl script that is used to convert the prolog to TCAP
   */
  const static std::string prologToTCAPScript;

  /**
   * Writes out the provided prolog rules
   * @param rules - the rules
   */
  void writePrologRules(std::vector<std::string> &rules) const;

  /**
   * Writes out the prologToTCAPScript
   */
  void writePrologToTCAPScript() const;

  /**
   * Converts a vector of strings to a vector of char*
   * @param prologArguments - the vector of strings
   * @return the returned char* vector
   */
  std::vector<char *> toArgv(const std::vector<std::string> &prologArguments) const;
};

#endif //PDB_PROLOGOPTIMIZER_H
