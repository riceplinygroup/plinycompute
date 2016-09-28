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

#include <memory>

#include "SelectionIr.h"

//#include "FilterQueryProcessor.h"
#include "InterfaceFunctions.h"

using std::make_shared;

//using pdb::FilterQueryProcessor;
using pdb::makeObject;

namespace pdb_detail
{

    SelectionIr::SelectionIr()
    {
    }

    SelectionIr::SelectionIr(shared_ptr<SetExpressionIr> inputSet, shared_ptr<RecordPredicateIr> condition,
                             SimpleSingleTableQueryProcessorPtr pageProcessor)
            : _inputSet(inputSet), _condition(condition)
    {
    }


    void SelectionIr::execute(SetExpressionIrAlgo &algo)
    {
        algo.forSelection(*this);
    }

    shared_ptr<SetExpressionIr> SelectionIr::getInputSet()
    {
       return _inputSet;
    }

    shared_ptr<RecordPredicateIr> SelectionIr::getCondition()
    {
        return _condition;
    }

//    template <class Output, class Input>
//    SimpleSingleTableQueryProcessorPtr SelectionIr::makeProcessor(Handle<Input> inputPlaceholder)
//    {
//        Lambda<bool> lambdaCondition = _condition->toLambda(inputPlaceholder);
//        return make_shared<FilterQueryProcessor<Output,Input>>(lambdaCondition);
//    }

}