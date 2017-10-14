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

#ifndef LAMBDA_TYPE_H
#define LAMBDA_TYPE_H

#include <vector>
#include <functional>
#include <iostream>
#include <memory>

#include "FuncType.h"
#include "Handle.h"
#include "Object.h"
#include "Lambda.h"

namespace pdb {

// an instance of this class lives inside each of the different lambda types, and provides
// the basic functionality.  There a concre version of one of these associated with each lambda type
template <typename Out>
class LambdaType {

public:
    virtual std::string toString() = 0;
    virtual std::function<Out()> getFunc() = 0;
    virtual std::vector<Handle<Object>*> getParams() = 0;
    virtual void addParam(Handle<Object>* addMe) = 0;
    virtual FuncType getType() = 0;
    virtual ~LambdaType(){};
};
}

#endif
