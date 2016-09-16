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
#ifndef PDB_QUERYINTERMEDIARYREP_RECORDPREDICATEIR_H
#define PDB_QUERYINTERMEDIARYREP_RECORDPREDICATEIR_H

#include "Handle.h"
#include "Lambda.h"
#include "Object.h"
#include "QueryNodeIr.h"

using pdb::Handle;
using pdb::Lambda;
using pdb::Object;

namespace pdb_detail
{
    /**
     * A boolean function that operates over a single input record.
     */
    class RecordPredicateIr : public QueryNodeIr
    {

    public:

        // contract from super
        void execute(QueryNodeIrAlgo &algo);

        /**
         * Produces a Lambda<bool> representation of the predicate.
         *
         * @param inputRecordPlaceholder a placeholder to represend the "free variable" input record the predicate
         *                               operates over within the structure of the returned Lambda.
         * @return a Lambda version of the predciate.
         */
        virtual Lambda<bool> toLambda(Handle<Object> &inputRecordPlaceholder) = 0;

    };
}

#endif //PDB_QUERYINTERMEDIARYREP_RECORDPREDICATEIR_H
