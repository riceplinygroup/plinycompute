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
#ifndef TOP_K_COMP
#define TOP_K_COMP

#include "ClusterAggregateComp.h"
#include "TopKQueue.h"
#include "LambdaCreationFunctions.h"

namespace pdb {

// implements a top-K computation.  The result of the computation is an object of type TopKQueue
// <Score, ValueType>;
// this queue stores the items with the top-K largest Score values.  The top-K computation takes as
// input a set of
// InputClass objects... to customize this class to perform a specific top-K computation, the user
// needs to override
// the getValueProjection () method so that it returns a TopKQueue object, initialized with: the
// value of k (the
// number of items to return), the Score object extracted from aggMe, and the ValueType object
// extracted from aggMe.
template <class InputClass, class Score, class ValueType>
class TopKComp : public ClusterAggregateComp<TopKQueue<Score, ValueType>,
                                             InputClass,
                                             int,
                                             TopKQueue<Score, ValueType>> {

public:
    Lambda<int> getKeyProjection(Handle<InputClass> aggMe) final {
        return makeLambda(aggMe, [&](Handle<InputClass>& aggMe) { return 1; });
    }

    // this method should be over-ridden in almost any implementation of a top-K query
    Lambda<TopKQueue<Score, ValueType>> getValueProjection(Handle<InputClass> aggMe) override {
        return makeLambda(aggMe, [&](Handle<InputClass>& aggMe) {
            return TopKQueue<Score, ValueType>(1, Score(), ValueType());
        });
    }
};
}

#endif
