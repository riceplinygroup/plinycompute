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
#ifndef PDB_TCAPPARSER_TABLEEXPRESSION_H
#define PDB_TCAPPARSER_TABLEEXPRESSION_H

#include <memory>
#include <functional>

using std::function;
using std::shared_ptr;

namespace pdb_detail
{
    class LoadOperation;   // forward declaration for match

    class ApplyOperation;  // forward declaration for match

    class FilterOperation; // forward declaration for match

    class HoistOperation;  // forward declaration for match

    class BinaryOperation; // forward declaration for match

    /**
     * A table value.
     */
    class TableExpression
    {
    public:

        /**
         * If the TableExpression is an instance of LoadOperation, calls forLoad.
         * If the TableExpression is an instance of ApplyOperation, calls forApply.
         * If the TableExpression is an instance of FilterOperation, calls forFilter.
         * If the TableExpression is an instance of HoistOperation, calls forHoist.
         * If the TableExpression is an instance of BinaryOperation, calls forBinaryOp.
         *
         * @param forLoad the load case
         * @param forApply the apply case
         * @param forFilter the filter case
         * @param forHoist the hoist case
         * @param forBinaryOp  the binaryOp case
         */
        virtual void match(function<void(LoadOperation &)> forLoad, function<void(ApplyOperation &)> forApply,
                           function<void(FilterOperation &)> forFilter, function<void(HoistOperation &)> forHoist,
                           function<void(BinaryOperation &)> forBinaryOp) = 0;
    };

    typedef shared_ptr<TableExpression> TableExpressionPtr;
}

#endif //PDB_TCAPPARSER_TABLEEXPRESSION_H
