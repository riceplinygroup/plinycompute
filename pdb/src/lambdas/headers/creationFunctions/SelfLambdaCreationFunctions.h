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

#ifndef PDB_SELFLAMBDA_H
#define PDB_SELFLAMBDA_H

#include <Handle.h>
#include <Ptr.h>
#include <SelfLambda.h>
#include <LambdaTree.h>

namespace pdb {

/**
 * creates a PDB lambda that simply returns the argument itself
 * @tparam ClassType // TODO add proper description
 * @param var // TODO add proper description
 * @return // TODO add proper description
 */
template <typename ClassType> LambdaTree<Ptr<ClassType>> makeLambdaFromSelf(Handle<ClassType>& var) {
  PDB_COUT << "makeLambdaFromSelf: input type code is " << var.getExactTypeInfoValue() << std::endl;
  return LambdaTree<Ptr<ClassType>>(std::make_shared<SelfLambda<ClassType>>(var));
}

}

#endif //PDB_SELFLAMBDA_H
