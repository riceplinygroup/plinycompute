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
#ifndef PDB_TCAPPARSER_GREATERTHANOP_H
#define PDB_TCAPPARSER_GREATERTHANOP_H

#include <functional>
#include <memory>

#include "BinaryOperation.h"
#include "BuildTcapIrTests.h"
#include "RetainClause.h"
#include "TcapIdentifier.h"

using std::function;
using std::shared_ptr;

namespace pdb_detail
{
    /**
     * Models a a greater than operation in the TCAP grammar.  For example:
     *
     *    C[examAverage] > C[hwAverage] retain all
     *
     * In this example:
     *
     *     lhsTableName would be C
     *     lhsColumnName would be examAverage
     *     rhsTableName would be C
     *     rhsColumnName would be hwAverage
     *     an instance of RetailAllClause would be the value of retain
     */
    class GreaterThanOp : public BinaryOperation
    {
    public:

        /**
         * The table from which the left hand column is drawn.
         */
        const TcapIdentifier lhsTableName;

        /**
         * The left hand side column used in comparision.
         */
        const TcapIdentifier lhsColumnName;

        /**
         * The table from which the right hand column is drawn.
         */
        const TcapIdentifier rhsTableName;

        /**
         * The right hand side column used in comparision.
         */
        const TcapIdentifier rhsColumnName;

        /**
         * The retention clause of the operation.
         */
        const shared_ptr<RetainClause> retain;

        // contract from super
        void execute(function<void(GreaterThanOp&)> forGreaterThan) override;

    private:

        /**
         * Creates a new GreaterThanOp.
         *
         * Throws invalid_argument exception if retain is nulltpr
         *
         * @param lhsTableName The table from which the left hand column is drawn.
         * @param lhsColumnName The left hand side column used in comparision.
         * @param rhsTableName The table from which the right hand column is drawn.
         * @param rhsColumnName The right hand side column used in comparision.
         * @param retain The retention clause of the operation.
         * @return a GreaterThanOp.
         */
        // private because throws exception and PDB style guide forbids exceptions from crossing API boundaries.
        GreaterThanOp(TcapIdentifier lhsTableName, TcapIdentifier lhsColumnName, TcapIdentifier rhsTableName,
                      TcapIdentifier rhsColumnName, shared_ptr<RetainClause> retain);

        friend BinaryOperationPtr makeBinaryOperation(class TcapTokenStream &tokens); // for constructor

        friend void pdb_tests::buildTcapIrTest6(class::UnitTest &qunit); // for constructor

    };
}

#endif //PDB_TCAPPARSER_GREATERTHANOP_H
