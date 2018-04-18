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

#ifndef JOIN_COMP
#define JOIN_COMP

#include "Computation.h"
#include "JoinTests.h"
#include "ComputePlan.h"
#include "JoinTuple.h"
#include "AbstractJoinComp.h"
#include "MultiInputsBase.h"
#include "PageCircularBufferIterator.h"
#include "DataProxy.h"
#include "PDBPage.h"
#include "JoinCompBase.h"

namespace pdb {

/**
 * // TODO Add proper description
 * @tparam Out - the output type
 * @tparam In1 - the first input type
 * @tparam In2 - the second input type
 * @tparam Rest - the rest of the input types
 */
template <typename Out, typename In1, typename In2, typename... Args>
class JoinComp : public JoinCompBase<Out, In1, In2, Args... > {

  /**
   * The computation returned by this method is called to see if a data item should be returned in the output set
   * @param in1 - first input
   * @param in2 - second input
   * @param otherArgs - the rest of the inputs
   * @return the projection lambda
   */
  virtual Lambda<bool> getSelection(Handle<In1> in1, Handle<In2> in2, Handle<Args>... otherArgs) = 0;

  /**
   * The computation returned by this method is called to produce output tuples from this method
   * @param in1 - first input
   * @param in2 - second input
   * @param otherArgs - the rest of the inputs
   * @return the projection lambda
   */
  virtual Lambda<Handle<Out>> getProjection(Handle<In1> in1, Handle<In2> in2, Handle<Args>... otherArgs) = 0;
};
}

#endif
