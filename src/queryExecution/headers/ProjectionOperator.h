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
#ifndef PROJECTION_OPERATOR_H
#define PROJECTION_OPERATOR_H

//by Jia, Sept 2016

#include "QueryBase.h"
#include "Selection.h"
#include "SimpleSingleTableQueryProcessor.h"

namespace pdb {

/*
 * this class encapsulates a ProjectionOperator that runs in backend.
 */

class ProjectionOperator : public ExecutionOperator {

    public:

       ProjectionOperator(Handle<QueryBase> selection) {
            this->selection = selection;           
       }

       std :: string getName() override {
            return "ProjectionOperator";
       }

       SimpleSingleTableQueryProcessorPtr getProcessor() override {
            return (unsafeCast<Selection<Object, Object>>(selection))->getProjectionProcessor();
       }

    private:
       Handle<QueryBase> selection;

};

}

#endif
