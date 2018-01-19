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

#ifndef SET_H
#define SET_H

#include <vector>
#include <functional>
#include <iostream>
#include <memory>
#include "Query.h"

// PRELOAD %Set <Nothing>%

namespace pdb {

// this corresponds to a database set
template <typename Out>
class Set : public Query<Out> {

public:
    ENABLE_DEEP_COPY

    Set() {}
    ~Set() {}

    Set(bool isError) {
        this->setError();
    }

    Set(std::string dbName, std::string setName) {
        this->setDBName(dbName);
        this->setSetName(setName);
    }

    void match(function<void(QueryBase&)> forSelection,
               function<void(QueryBase&)> forSet,
               function<void(QueryBase&)> forQueryOutput) override {
        forSet(*this);
    };

    // gets the number of inputs
    virtual int getNumInputs() override {
        return 0;
    }

    // gets the name of the i^th input type...
    virtual std::string getIthInputType(int i) override {
        return "I have no inputs!!";
    }

    virtual std::string getQueryType() override {
        return "set";
    }
};
}

#endif
