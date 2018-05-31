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

#ifndef SELECTION_COMP
#define SELECTION_COMP

#include "SelectionCompBase.h"

namespace pdb {

template<class OutputClass, class InputClass>
class SelectionComp : public SelectionCompBase<OutputClass, InputClass> {
public:

  /**
   * The computation returned by this method is called to see if a data item should be returned in the output set
   * @param checkMe
   * @return
   */
  virtual Lambda<bool> getSelection(Handle<InputClass> checkMe) = 0;

  /**
   * the computation returned by this method is called to perfom a transformation on the input
   * item before it is inserted into the output set
   * @param checkMe
   * @return
   */
  virtual Lambda<Handle<OutputClass>> getProjection(Handle<InputClass> checkMe) = 0;

};
}

#endif
