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

#include "SelectionIr.h"

#include "InterfaceFunctions.h"

using pdb::makeObject;

namespace pdb_detail
{

    Handle<SelectionIr> SelectionIr::make(Handle<SetExpressionIr> inputSet, Handle<RecordPredicateIr> condition)
    {
        return makeObject<SelectionIr>(inputSet, condition);
    }

    SelectionIr::SelectionIr(Handle<SetExpressionIr> inputSet, Handle<RecordPredicateIr> condition)
            : _inputSet(inputSet), _condition(condition)
    {
    }

    void SelectionIr::execute(QueryNodeIrAlgo &algo)
    {
        algo.forSelection(*this);
    }

    void SelectionIr::execute(SetExpressionIrAlgo &algo)
    {
        algo.forSelection(*this);
    }

    Handle<SetExpressionIr> SelectionIr::getInputSet()
    {
       return _inputSet;
    }

    Handle<RecordPredicateIr> SelectionIr::getCondition()
    {
        return _condition;
    }

}