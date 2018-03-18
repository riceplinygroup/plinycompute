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

#ifndef MULTISELECTION_COMP_H
#define MULTISELECTION_COMP_H

#include "MultiSelectionCompBase.h"

namespace pdb {

/**
 * TODO add proper description
 * @tparam OutputClass
 * @tparam InputClass
 */
template<class OutputClass, class InputClass>
class MultiSelectionComp : public MultiSelectionCompBase<OutputClass, InputClass> {

 public:

  /**
   * the computation returned by this method is called to see if a data item should be returned in the output set
   * @param checkMe
   * @return
   */
  virtual pdb::Lambda<bool> getSelection(pdb::Handle<InputClass> checkMe) = 0;

  /**
   * the computation returned by this method is called to produce output tuples from this method
   * @param checkMe
   * @return
   */
  virtual pdb::Lambda<pdb::Vector<pdb::Handle<OutputClass>>> getProjection(pdb::Handle<InputClass> checkMe) = 0;

};
}

#endif
