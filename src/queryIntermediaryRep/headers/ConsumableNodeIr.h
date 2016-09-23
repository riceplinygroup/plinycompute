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
#ifndef PDB_PDB_QUERYINTERMEDIARYREP_CONSUMABLENODEIR_H
#define PDB_PDB_QUERYINTERMEDIARYREP_CONSUMABLENODEIR_H

#include "InterfaceFunctions.h"
#include "QueryNodeIr.h"

using pdb::makeObject;

namespace pdb_detail
{
    class ConsumableNodeIr : public QueryNodeIr
    {
    public:


        void addConsumer(Handle<QueryNodeIr> consumer)
        {
            // TODO: check for duplicate before adding

            _consumers.push_back(consumer);
        }

        uint32_t getConsumerCount()
        {
            return _consumers.size();
        }

        Handle<QueryNodeIr> getConsumer(uint32_t index)
        {
            return _consumers[index];
        }


    private:

        // TODO: this is probably better as a pdb::Set once that type exists.
        Vector<Handle<QueryNodeIr>> _consumers;
    };
}

#endif //PDB_PDB_QUERYINTERMEDIARYREP_CONSUMABLENODEIR_H
