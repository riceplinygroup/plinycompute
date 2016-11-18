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
#ifndef PDB_TCAPPARSER_LOADOPERATION_H
#define PDB_TCAPPARSER_LOADOPERATION_H

#include <memory>
#include <string>

#include "TableExpression.h"

using std::shared_ptr;
using std::string;

namespace pdb_detail
{
    /**
     * Models a LoadOperation in the TCAP grammar.  For example:
     *
     *    load "(databaseName, inputSetName)"
     *
     * In this example:
     *
     *     source would be (databaseName, inputSetName)
     */
    class LoadOperation : public TableExpression
    {
    public:

        /**
         * The source of the load.
         */
        const string source;

        /**
         * Creates a new LoadOperation.
         * @param source The source of the load.
         * @return the new LoadOperation
         */
        LoadOperation(const string &source);

        // contract from super
        void match(function<void(LoadOperation &)> forLoad, function<void(ApplyOperation &)>,
                   function<void(FilterOperation &)>, function<void(HoistOperation &)>,
                   function<void(BinaryOperation &)>) override;

    };

    typedef shared_ptr<LoadOperation> LoadOperationPtr;
}

#endif //PDB_TCAPPARSER_LOADOPERATION_H
