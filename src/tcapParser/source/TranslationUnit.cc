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
#include "TranslationUnit.h"

using std::make_shared;
using std::invalid_argument;

namespace pdb_detail {
/**
 * @param statement the statement to wrap in a vector.
 * @return a vector containing only statement
 */
shared_ptr<vector<TcapStatementPtr>> wrapInVector(TcapStatementPtr statement) {
    shared_ptr<vector<shared_ptr<TcapStatement>>> stmts =
        make_shared<vector<shared_ptr<TcapStatement>>>();
    stmts->push_back(statement);
    return stmts;
}

TranslationUnit::TranslationUnit(TcapStatementPtr statement)
    : statements(wrapInVector(statement)) {}

TranslationUnit::TranslationUnit(shared_ptr<const vector<TcapStatementPtr>> statements)
    : statements(statements) {
    if (statements == nullptr)
        throw invalid_argument("statements may not be null");
}
}
