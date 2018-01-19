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

#include "ProcessorFactoryFilterQueryProcessor.h"
#include "SelectionIr.h"

#include "Selection.h"
#include "InterfaceFunctions.h"

using std::make_shared;

using pdb::FilterQueryProcessor;
using pdb::makeObject;
using pdb::ProcessorFactoryFilterQueryProcessor;
using pdb::Selection;

namespace pdb_detail {

SelectionIr::SelectionIr() {}

SelectionIr::SelectionIr(shared_ptr<SetExpressionIr> inputSet, Handle<QueryBase> originalSelection)
    : _inputSet(inputSet), _originalSelection(originalSelection) {}

string SelectionIr::getName() {
    return "SelectionIr";
}

void SelectionIr::execute(SetExpressionIrAlgo& algo) {
    algo.forSelection(*this);
}

shared_ptr<SetExpressionIr> SelectionIr::getInputSet() {
    return _inputSet;
}

Handle<ProcessorFactory> SelectionIr::makeProcessorFactory() {
    return makeObject<ProcessorFactoryFilterQueryProcessor>(_originalSelection);
}

SimpleSingleTableQueryProcessorPtr SelectionIr::makeProcessor() {
    return make_shared<FilterQueryProcessor<Object, Object>>(_originalSelection);
}

Handle<QueryBase> SelectionIr::getQueryBase() {
    return _originalSelection;
}
}
