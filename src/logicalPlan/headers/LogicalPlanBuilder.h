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
#ifndef _LOGICALPLANBUILDER_H_
#define _LOGICALPLANBUILDER_H_

#include <memory>
#include <vector>

#include "Instruction.h"
#include "ParserTypes.h"

using std::shared_ptr;
using std::vector;

using pdb_detail::InstructionPtr;

/**
 * Translates each of the given instructions into an instance one of: Input, Output, Computation
 * and agregates the results into the returned LogicalPlan.
 *
 * Load turns into Input
 * Store turns into Output
 * everything other instruction variant turns into a Computation.
 *
 * @param instructions the list of instructions to translate
 * @return A LogicalPlan representation of the given instructions.
 */
shared_ptr<LogicalPlan> buildLogicalPlan(shared_ptr<vector<InstructionPtr>> instructions);

#endif