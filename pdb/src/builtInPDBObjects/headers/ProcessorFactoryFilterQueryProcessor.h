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
//
// Created by barnett on 10/2/16.
//

#ifndef PDB_PROCESSORFACTORYFILTERQUERYPROCESSOR_H
#define PDB_PROCESSORFACTORYFILTERQUERYPROCESSOR_H

#include "Selection.h"
#include "Handle.h"
#include "ProcessorFactory.h"
#include "QueryBase.h"

using std::make_shared;

namespace pdb {
class ProcessorFactoryFilterQueryProcessor : public ProcessorFactory {
public:
    ProcessorFactoryFilterQueryProcessor() {
        // for deep copy
    }

    ProcessorFactoryFilterQueryProcessor(Handle<QueryBase> originalSelection)
        : _originalSelection(originalSelection) {}

    SimpleSingleTableQueryProcessorPtr makeProcessor() override {
        return make_shared<FilterQueryProcessor<Object, Object>>(_originalSelection);
    }

    ENABLE_DEEP_COPY

private:
    Handle<QueryBase> _originalSelection;
};
}

#endif  // PDB_PROCESSORFACTORYFILTERQUERYPROCESSOR_H
