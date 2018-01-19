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
#ifndef PDB_TCAPPARSER_TRANSLATIONUNIT_H
#define PDB_TCAPPARSER_TRANSLATIONUNIT_H

#include <memory>
#include <vector>

#include "TcapStatement.h"
#include "SafeResult.h"

using std::make_shared;
using std::shared_ptr;
using std::vector;

namespace pdb_detail {
/**
 * A list of TCAP statements.
 */
class TranslationUnit {

public:
    /**
     * The statements of the unit.
     */
    const shared_ptr<const vector<TcapStatementPtr>> statements;

    /**
     * Creates a single statement unit.
     *
     * @param statement the statement
     * @return A new TranslationUnit
     */
    TranslationUnit(TcapStatementPtr statement);

private:
    /**
     * Creates a new TranslationUnit
     *
     * If statements is nullptr, throws invalid_argument exception.
     *
     * @param statements the statements of the unit.
     * @return a new TranslationUnit
     */
    // private because throws exception and PDB style guide forbids exceptions crossing API
    // boundaries.
    TranslationUnit(shared_ptr<const vector<TcapStatementPtr>> statements);

    friend shared_ptr<pdb::SafeResult<TranslationUnit>> parseTcap(
        const string& source);  // for constructor
};
}

#endif  // PDB_TCAPPARSER_TRANSLATIONUNIT_H
