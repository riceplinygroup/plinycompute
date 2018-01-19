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

#ifndef TUPLE_SPEC_H
#define TUPLE_SPEC_H

#include <iostream>
#include <memory>
#include <stdlib.h>
#include <string>
#include <utility>
#include <vector>
#include <map>

#include "AttList.h"

// and here is the specifier for a TupleSet... it is basically a bunch of attribute
// names, as well as the name of the TupleSet
struct TupleSpec {

private:
    std::string setName;
    std::vector<std::string> atts;

public:
    TupleSpec() {
        setName = std::string("Empty");
    }

    ~TupleSpec() {}

    TupleSpec(std::string setNameIn) {
        setName = setNameIn;
    }

    TupleSpec(std::string setNameIn, AttList& useMe) {
        setName = setNameIn;
        atts = useMe.atts;
    }

    std::string& getSetName() {
        return setName;
    }

    std::vector<std::string>& getAtts() {
        return atts;
    }

    bool operator==(const TupleSpec& toMe) {
        return setName == toMe.setName;
    }

    friend std::ostream& operator<<(std::ostream& os, const TupleSpec& printMe);
};

inline std::ostream& operator<<(std::ostream& os, const TupleSpec& printMe) {
    os << printMe.setName << " (";
    bool first = true;
    for (auto& a : printMe.atts) {
        if (!first)
            os << ", ";
        first = false;
        os << a;
    }
    os << ")";
    return os;
}

#endif
