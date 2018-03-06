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

#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include <memory>
#include "Lambda.h"
#include "LambdaTypes.h"

namespace pdb {

// checks for equality
template <class L, class R>
Lambda<bool> operator==(Lambda<L> left, Lambda<R> right) {
    return Lambda<bool>(std::make_shared<EqualsOp<L, R>>(left, right));
}

// checks if the LHS is greater than the right
template <class L, class R>
Lambda<bool> operator<(Lambda<L> left, Lambda<R> right) {
    return Lambda<bool>(std::make_shared<GreaterThanOp<L, R>>(left, right));
}

// adds two values
template <class T>
Lambda<T> operator+(Lambda<T> left, Lambda<T> right) {
    return Lambda<T>(std::make_shared<PlusOp<T>>(left, right));
}
}

#endif
