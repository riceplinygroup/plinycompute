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
#include "SimpleSingleTableQueryProcessor.h"
#include "Object.h"
#include <string>

namespace pdb {

/*
 * this class encapsulates an Operator that can run in backend, now we support following operators:
 * Selection/Filtering
 * Projection
 */

class ExecutionOperator : public Object {

    public:

       virtual string getName() = 0;
       virtual SimpleSingleTableQueryProcessorPtr getProcessor() = 0;
};

}

#endif
