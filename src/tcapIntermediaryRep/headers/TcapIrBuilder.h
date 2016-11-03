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
#ifndef PDB_TCAPINTERMEDIARYREP_TCAPIRBUILDER_H
#define PDB_TCAPINTERMEDIARYREP_TCAPIRBUILDER_H

#include <algorithm>
#include <memory>
#include <vector>

#include "ApplyFunction.h"
#include "Filter.h"
#include "GreaterThan.h"
#include "Hoist.h"
#include "Instruction.h"
#include "Load.h"
#include "TcapParser.h"

using std::shared_ptr;
using std::vector;

using pdb_detail::Filter;
using pdb_detail::Hoist;
using pdb_detail::Load;

namespace pdb_detail
{
     shared_ptr<vector<shared_ptr<Instruction>>> buildTcapIr(shared_ptr<TranslationUnit> unit);
}

#endif //PDB_TCAPINTERMEDIARYREP_TCAPIRBUILDER_H
