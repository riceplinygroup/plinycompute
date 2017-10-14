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
#include "TcapStatement.h"

using std::make_shared;
using std::invalid_argument;

namespace pdb_detail {
/**
 * @param attribute the attribute to wrap in a vector
 * @return a vector of only attribute
 */
shared_ptr<vector<TcapAttribute>> wrapInVector(TcapAttribute attribute) {
    shared_ptr<vector<TcapAttribute>> attributes = make_shared<vector<TcapAttribute>>();
    attributes->push_back(attribute);
    return attributes;
}

TcapStatement::TcapStatement(shared_ptr<vector<TcapAttribute>> attributes)
    : attributes(attributes) {
    if (attributes == nullptr)
        throw invalid_argument("attributes may not be null");
}

TcapStatement::TcapStatement(TcapAttribute attribute) : attributes(wrapInVector(attribute)) {}
}
