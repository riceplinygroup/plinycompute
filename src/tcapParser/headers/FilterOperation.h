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
#ifndef PDB_TCAPPARSER_FILTEROPERATION_H
#define PDB_TCAPPARSER_FILTEROPERATION_H

#include <memory>
#include <string>

#include "RetainClause.h"
#include "TableExpression.h"
#include "TcapIdentifier.h"

using std::shared_ptr;
using std::string;

namespace pdb_detail
{
    /**
     * Models a filter opertion in the TCAP grammar.  For example:
     *
     *    filter D by isExamGreater retain all
     *
     * In this example:
     *
     *     inputTableName would be D
     *     filterColumnName would be isExamGreater
     *     an instance of RetailAllClause would be the value of retain
     */
    class FilterOperation : public TableExpression
    {
    public:

        /**
         * The name of the table filterd by the operation.
         */
        const TcapIdentifier inputTableName;

        /**
         * The name of the column in inputTableName to be filtered upon.
         */
        const TcapIdentifier filterColumnName;

        /**
         * The retention clause of the operation.
         */
        const shared_ptr<RetainClause> retain;

        /**
         * Creates a new FilterOperation.
         *
         * @param inputTableName The name of the table filterd by the operation.
         * @param filterColumnName The name of the column in inputTableName to be filtered upon.
         * @param retain The retention clause of the operation.
         * @return a new FilterOperation
         */
        FilterOperation(TcapIdentifier inputTableName, TcapIdentifier filterColumnName, shared_ptr<RetainClause> retain);

        // contract from super
        void match(function<void(LoadOperation &)>, function<void(ApplyOperation &)>,
                   function<void(FilterOperation &)> forFilter, function<void(HoistOperation &)>,
                   function<void(BinaryOperation &)>) override ;
    };

    typedef shared_ptr<FilterOperation> FilterOperationPtr;
}

#endif //PDB_TCAPPARSER_FILTEROPERATION_H
