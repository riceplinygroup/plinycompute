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
#include "ProjectionIr.h"

#include "InterfaceFunctions.h"
#include "ProcessorFactoryProjectionQueryProcessor.h"

using pdb::makeObject;
using pdb::ProcessorFactoryProjectionQueryProcessor;

namespace pdb_detail {

const string ProjectionIr::PROJECTION_IR = "ProjectionIr";

ProjectionIr::ProjectionIr(shared_ptr<SetExpressionIr> inputSet,
                           Handle<QueryBase> originalSelection)
    : _inputSet(inputSet), _originalSelection(originalSelection) {}

string ProjectionIr::getName() {
    return PROJECTION_IR;
}

void ProjectionIr::execute(SetExpressionIrAlgo& algo) {
    algo.forProjection(*this);
}


shared_ptr<SetExpressionIr> ProjectionIr::getInputSet() {
    return _inputSet;
}


Handle<QueryBase> ProjectionIr::getQueryBase() {
    return _originalSelection;
}

Handle<ProcessorFactory> ProjectionIr::makeProcessorFactory() {
    return makeObject<ProcessorFactoryProjectionQueryProcessor>(_originalSelection);
}
}
