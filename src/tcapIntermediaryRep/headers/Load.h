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
#ifndef PDB_TCAPINTERMEDIARYREP_LOAD_H
#define PDB_TCAPINTERMEDIARYREP_LOAD_H

#include "ApplyFunction.h"
#include "Instruction.h"

namespace pdb_detail
{
    class Load : public Instruction
    {
    public:

        const string outputColumnId;

        const string outputTableId;

        const string source;

        Load(string outputTableId, string outputColumnId, string source);

        void match(function<void(Load&)> forLoad, function<void(ApplyFunction&)>, function<void(ApplyMethod&)>,
                   function<void(Filter&)>, function<void(Hoist&)>, function<void(GreaterThan&)>,
                   function<void(Store&)> forStore) override;
    };

    typedef shared_ptr<Load> LoadPtr;

    LoadPtr makeLoad(string outputTableId, string outputColumnId, string source);

}

#endif //PDB_TCAPINTERMEDIARYREP_LOAD_H
