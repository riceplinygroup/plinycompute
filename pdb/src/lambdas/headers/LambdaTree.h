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

#include <memory>
#include <vector>
#include <functional>
#include <mustache.h>
#include <mustache_helper.h>
#include "Object.h"
#include "Handle.h"
#include "Ptr.h"
#include "TupleSpec.h"
#include "ComputeExecutor.h"
#include "SimpleComputeExecutor.h"
#include "ComputeInfo.h"
#include "MultiInputsBase.h"
#include "TupleSetMachine.h"


#ifndef PDB_LAMBDATREE_H
#define PDB_LAMBDATREE_H

namespace pdb {

template<typename Out>
class TypedLambdaObject;

/**
 * Internally, the query object creates a "class LambdaTree <Out>" object.
 * The reason that the query internally constructs a "class LambdaTree <Out>" whereas the query returns a
 * "class Lambda <Out>" is that there may be a mismatch between the two type parameters---the
 * LambdaTree may return a "class LambdaTree <Ptr<Out>>" object for efficiency.  Thus, we allow
 * a "class Lambda <Out>" object to be constructed with either a  "class LambdaTree <Out>"
 * or a  "class LambdaTree <Ptr<Out>>".  We don't want to allow implicit conversions between
 *  "class LambdaTree <Out>" and "class LambdaTree <Ptr<Out>>", however, which is why we need
 * the separate type.
 * Each "class LambdaTree <Out>" object is basically just a wrapper for a shared_ptr to a
 * "TypedLambdaObject <Out> object".  So that we can pass around pointers to these things (without
 * return types), "TypedLambdaObject <Out>" derives from "GenericLambdaObject".
 * @tparam ReturnType // TODO missing description
 */
template<typename ReturnType>
class LambdaTree {

 public:

  LambdaTree() = default;

  /**
   * // TODO missing description
   * @param toMe - // TODO missing description
   */
  LambdaTree(const LambdaTree<ReturnType> &toMe) : me(toMe.me) {}

  /**
   * // TODO missing description
   * @param i - // TODO missing description
   * @return - // TODO missing description
   */
  unsigned int getInputIndex(int i) {
    return me->getInputIndex(i);
  }

  /**
   * // TODO missing description
   * @return - // TODO missing description
   */
  auto &getPtr() {
    return me;
  }

  /**
   * // TODO missing description
   * @return - // TODO missing description
   */
  LambdaTree<ReturnType> *operator->() const {
    return me.get();
  }

  /**
   * // TODO missing description
   * @return - // TODO missing description
   */
  LambdaTree<ReturnType> &operator*() const {
    return *me;
  }

  /**
   * // TODO missing description
   * @tparam Type - // TODO missing description
   * @param meIn - // TODO missing description
   */
  template<class Type>
  explicit LambdaTree(std::shared_ptr<Type> meIn) {
    me = meIn;
  }

  /**
   * // TODO missing description
   * @param toMe - // TODO missing description
   * @return - // TODO missing description
   */
  LambdaTree<ReturnType> &operator=(const LambdaTree<ReturnType> &toMe) {
    me = toMe.me;
    return *this;
  }

  /**
   * // TODO missing description
   * @tparam Type - // TODO missing description
   * @param toMe - // TODO missing description
   * @return - // TODO missing description
   */
  template<class Type>
  LambdaTree<ReturnType> &operator=(std::shared_ptr<Type> toMe) {
    me = toMe;
    return *this;
  }


 private:

  /**
   * // TODO missing description
   */
  std::shared_ptr<TypedLambdaObject<ReturnType>> me;

};

}

#endif //PDB_LAMBDATREE_H
