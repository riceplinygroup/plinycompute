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
#ifndef PDB_TCAPPARSER_RETAINALLCLAUSE_H
#define PDB_TCAPPARSER_RETAINALLCLAUSE_H

#include <functional>

#include "RetainClause.h"

using std::function;

namespace pdb_detail {
/**
 * A retention clause that indicates that all columns from an input table should be copied into the
 * output table.
 */
class RetainAllClause : public RetainClause {
    /**
     * @return true
     */
    bool isAll() override;

    /**
     * @return false
     */
    bool isNone() override;

    // contract from super
    void match(function<void(RetainAllClause&)> forAll,
               function<void(RetainExplicitClause&)>,
               function<void(RetainNoneClause&)> forNone) override;
};
}

#endif  // PDB_TCAPPARSER_RETAINALLCLAUSE_H
