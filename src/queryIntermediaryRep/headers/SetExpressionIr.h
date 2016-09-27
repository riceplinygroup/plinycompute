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
#ifndef PDB_QUERYINTERMEDIARYREP_SETEXPRESSIONIR_H
#define PDB_QUERYINTERMEDIARYREP_SETEXPRESSIONIR_H

#include <list>

#include "Handle.h"
#include "ConsumableNodeIr.h"
#include "SetExpressionIrAlgo.h"
#include "PDBVector.h"

using pdb::Handle;
using pdb::Vector;

namespace pdb_detail
{
    /**
     * Base class for any class that models a PDB set.
     */
    class SetExpressionIr
    {

    public:

        /**
         * Executes the given algorithm on the expression.
         *
         * @param algo the algoithm to execute.
         */
        virtual void execute(SetExpressionIrAlgo &algo) = 0;


        void addConsumer(Handle<SetExpressionIr> consumer)
        {
            // TODO: check for duplicate before adding

            _consumers.push_back(consumer);
        }

        uint32_t getConsumerCount()
        {
            return _consumers.size();
        }

        Handle<SetExpressionIr> getConsumer(uint32_t index)
        {
            return _consumers[index];
        }


    private:

        // TODO: this is probably better as a pdb::Set once that type exists.
        Vector<Handle<SetExpressionIr>> _consumers;

    };
}

#endif //PDB_QUERYINTERMEDIARYREP_SETEXPRESSIONIR_H
