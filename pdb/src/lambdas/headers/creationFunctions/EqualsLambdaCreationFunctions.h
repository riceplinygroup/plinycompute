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

#ifndef PDB_EQUALSLAMBDA_H
#define PDB_EQUALSLAMBDA_H

#include <LambdaTree.h>
#include <EqualsLambda.h>

namespace pdb {

/**
 * creates a PDB lambda out of an == operator
 * @tparam LeftType // TODO add proper description
 * @tparam RightType // TODO add proper description
 * @param lhs // TODO add proper description
 * @param rhs // TODO add proper description
 * @return // TODO add proper description
 */
template <typename LeftType, typename RightType>
LambdaTree<bool> operator==(LambdaTree<LeftType> lhs, LambdaTree<RightType> rhs) {
  return LambdaTree<bool>(std::make_shared<EqualsLambda<LeftType, RightType>>(lhs, rhs));
}

}

#endif //PDB_EQUALSLAMBDA_H
