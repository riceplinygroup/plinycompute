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
#ifndef PDB_TCAPPARSER_RETAINEXPLICITCLAUSE_H
#define PDB_TCAPPARSER_RETAINEXPLICITCLAUSE_H

#include <memory>
#include <vector>

#include "RetainClause.h"
#include "TcapIdentifier.h"

using std::vector;
using std::shared_ptr;


namespace pdb_detail
{
    /**
     * A retention clause that indicates that specific enumerated columns from an input table should be
     * copied into the output table.
     */
    class RetainExplicitClause : public RetainClause
    {

    public:

        /**
         * The columns to be retained.
         */
        const shared_ptr<const vector<TcapIdentifier>> columns;

        /**
         * Creates a RetainExplicitClause that only retains one column.
         *
         * @param column the column to retain
         * @return a new RetainExplicitClause instance.
         */
        RetainExplicitClause(TcapIdentifier column);

        /**
         * @return false
         */
        bool isAll();

        /**
         * @return false
         */
        bool isNone();

        // contract from super
        void match(function<void(RetainAllClause&)>, function<void(RetainExplicitClause&)> forExplicit,
                   function<void(RetainNoneClause&)>);

    private:

        /**
         * Creates a RetainExplicitClause that only retains one column.
         *
         * If columns is nullptr, throws invalid_argument exception
         *
         * @param column the column to retain
         * @return a new RetainExplicitClause instance.
         */
        // private because throws exception and PDB style guide forbids throwing exception across API boundaries.
        RetainExplicitClause(shared_ptr<vector<TcapIdentifier>> columns);

        friend RetainClausePtr makeRetainClause(class TcapTokenStream &tokens); // for contructor
    };
}

#endif //PDB_TCAPPARSER_RETAINEXPLICITCLAUSE_H
