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

#ifndef JOIN_H
#define JOIN_H

#include "Handle.h"
#include "Lambda.h"
#include "Object.h"
#include "Query.h"

namespace pdb {

// this is the basic selection type... users derive from this class in order to write
// a selection query
template <typename Out, typename In1, typename In2, typename... Rest>
class Join : public Query<Out> {

    virtual Lambda<bool> getSelection(Handle<In1>& in1,
                                      Handle<In2>& in2,
                                      Handle<Rest>&... otherArgs) = 0;

    virtual Lambda<Handle<Out>> getProjection(Handle<In1>& in1,
                                              Handle<In2>& in2,
                                              Handle<Rest>&... otherArgs) = 0;

    // gets the number of intputs to this query type
    virtual int getNumInputs() override {
        return 2 + sizeof...(Rest);
    }

    // gets the name of the i^th input type...
    virtual std::string getIthInputType(int i) override {
        if (i == 0)
            return getTypeName<In1>();
        else if (i == 1)
            return getTypeName<In2>();
        else if (i >= getNumInputs())
            return "bad index";

        std::string nameArray[] = {getTypeName<Rest>()...};
        return nameArray[i - 2];
    }

    virtual std::string getQueryType() override {
        return "join";
    }
};
}

#endif
