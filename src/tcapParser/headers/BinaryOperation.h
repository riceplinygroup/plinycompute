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
#ifndef PDB_TCAPPARSER_BINARYOPERATION_H
#define PDB_TCAPPARSER_BINARYOPERATION_H

#include <functional>

#include "TableExpression.h"

using std::function;

namespace pdb_detail
{
    class GreaterThanOp;

    class BinaryOperation : public TableExpression
    {
    public:

        void execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)>,
                     function<void(FilterOperation&)>, function<void(HoistOperation&)>,
                     function<void(BinaryOperation&)> forBinaryOp);

        virtual void execute(function<void(GreaterThanOp&)> forGreaterThan) = 0;
    };

}

#endif //PDB_TCAPPARSER_BINARYOPERATION_H
