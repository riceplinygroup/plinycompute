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
#ifndef PDB_TCAPPARSER_HOISTOPERATION_H
#define PDB_TCAPPARSER_HOISTOPERATION_H

#include <memory>
#include <string>
#include <vector>

#include "RetainClause.h"
#include "TableExpression.h"
#include "TcapIdentifier.h"

using std::shared_ptr;
using std::string;
using std::vector;

namespace pdb_detail
{
    /**
     * Models a HoistOperation in the TCAP grammar.  For example:
     *
     *    hoist "homeworkAverage" from B[student] retain all
     *
     * In this example:
     *
     *     hoistTarget would be homeworkAverage
     *     inputTable would be B
     *     inputTableColumnName would be student
     *     an instance of RetailAllClause would be the value of retain
     */
    class HoistOperation : public TableExpression
    {
    public:

        /**
         * A descriptor of the field to be hoisted. This is metadata only and has no relationship to TCAP.
         */
        const string hoistTarget;

        /**
         * The table from which a column will be hoisted.
         */
        const TcapIdentifier inputTable;

        /**
         * The name of the column to hoist.
         */
        const TcapIdentifier inputTableColumnName;

        /**
         * The retention clause of the operation.
         */
        const shared_ptr<RetainClause> retain;

        /**
         * Creates a new HoistOperation
         *
         * @param hoistTarget A descriptor of the field to be hoisted.
         * @param inputTable The table from which a column will be hoisted.
         * @param inputTableColumnName The name of the column to hoist.
         * @param retain The retention clause of the operation.
         * @return a new HoistOperation
         */
        HoistOperation(string hoistTarget, TcapIdentifier inputTable, TcapIdentifier inputTableColumnName,
                       shared_ptr<RetainClause> retain);

        // contract from super
        void match(function<void(LoadOperation &)>, function<void(ApplyOperation &)>, function<void(FilterOperation &)>,
                   function<void(HoistOperation &)> forHoist, function<void(BinaryOperation &)>);
    };

    typedef shared_ptr<HoistOperation> HoistOperationPtr;


}

#endif //PDB_TCAPPARSER_HOISTOPERATION_H
