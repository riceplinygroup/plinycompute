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
#ifndef EXECUTION_OPERATOR_H
#define EXECUTION_OPERATOR_H

//by Jia, Sept 2016
#include "BlockQueryProcessor.h"
#include "Object.h"
#include "QueryBase.h"
#include <string>

// PRELOAD %ExecutionOperator%

namespace pdb {

/*
 * this class encapsulates an Operator that can run in backend, now we support following operators:
 * Selection/Filtering
 * Projection
 */

class ExecutionOperator : public Object {

    public:


       virtual string getName() = 0;
       virtual BlockQueryProcessorPtr getProcessor() = 0;
       virtual Handle<QueryBase>& getSelection() = 0;
};

}

#endif
