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
#include "RetainExplicitClause.h"

#include <vector>

using std::make_shared;
using std::vector;

using std::invalid_argument;

namespace pdb_detail {
/**
 * @param ident the ident to wrap in a vector
 * @return a vector containnig only ident
 */
shared_ptr<vector<TcapIdentifier>> wrapInVector(TcapIdentifier ident) {
    shared_ptr<vector<TcapIdentifier>> idents = make_shared<vector<TcapIdentifier>>();
    idents->push_back(ident);
    return idents;
}


RetainExplicitClause::RetainExplicitClause(shared_ptr<vector<TcapIdentifier>> columns)
    : columns(columns) {
    if (columns == nullptr)
        throw invalid_argument("columns may not be null");
}

RetainExplicitClause::RetainExplicitClause(TcapIdentifier column) : columns(wrapInVector(column)) {}

bool RetainExplicitClause::isAll() {
    return false;
}

bool RetainExplicitClause::isNone() {
    return false;
}

void RetainExplicitClause::match(function<void(RetainAllClause&)>,
                                 function<void(RetainExplicitClause&)> forExplicit,
                                 function<void(RetainNoneClause&)>) {
    forExplicit(*this);
}
}
