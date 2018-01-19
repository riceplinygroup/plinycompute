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

#ifndef ATT_LIST_H
#define ATT_LIST_H

#include <iostream>
#include <memory>
#include <stdlib.h>
#include <string>
#include <utility>
#include <vector>
#include <map>

// this is a list of attributes... used to build up schema specifications
struct AttList {

private:
    std::vector<std::string> atts;

public:
    ~AttList() {}
    AttList() {}

    void appendAttribute(char* appendMe) {
        atts.push_back(std::string(appendMe));
    }

    std::vector<std::string>& getAtts() {
        return atts;
    }

    friend struct TupleSpec;
};

#endif
