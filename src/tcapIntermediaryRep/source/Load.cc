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
#include "Load.h"

namespace pdb_detail
{
    Load::Load(string outputTableId, string outputColumnId, string source)
    :Instruction(InstructionType::load), outputColumnId(outputColumnId), outputTableId(outputTableId),
    source(source)
    {
    }

    void Load::match(function<void(Load&)> forLoad, function<void(ApplyFunction&)>, function<void(ApplyMethod&)>,
               function<void(Filter&)>, function<void(Hoist&)>, function<void(GreaterThan&)>,
               function<void(Store&)>)
    {
        forLoad(*this);
    }

    LoadPtr makeLoad(string outputTableId, string outputColumnId, string source)
    {
        return make_shared<Load>(outputTableId, outputColumnId, source);
    }
}