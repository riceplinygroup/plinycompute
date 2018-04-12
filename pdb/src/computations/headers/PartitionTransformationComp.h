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

#ifndef PARTITION_TRANSFORMATION_COMP
#define PARTITION_TRANSFORMATION_COMP

#include "PartitionTransformationCompBase.h"

namespace pdb {

template<class KeyClass, class ValueClass>
class PartitionTransformationComp : public PartitionTransformationCompBase<KeyClass, ValueClass> {
public:

  /**
   * the computation returned by this method is called to perfom a transformation on the input
   * item before it is inserted into the output set to decide which partition the input item should
   * be stored.
   * @param checkMe: the input element
   * @return: the output lambda tree used to apply to the input element
   */
  virtual Lambda<KeyClass> getProjection(Handle<ValueClass> checkMe) = 0;

};
}

#endif
