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

#ifndef SELECTION_H
#define SELECTION_H

#include "Handle.h"
#include "SimpleLambda.h"
#include "Object.h"
#include "Query.h"
#include "SetOperation.h"
#include "QueryAlgo.h"
#include "SimpleSingleTableQueryProcessor.h"
#include "BlockQueryProcessor.h"

namespace pdb {

// this is the basic selection type... users derive from this class in order to write
// a selection query
template <typename Out, typename In>
class Selection : public Query<Out> {

public:
    // over-ridden by the user so they can supply the actual selection predicate
    virtual SimpleLambda<bool> getSelection(Handle<In>& in) = 0;

    // over-ridden by the user so they can supply the selection on projection
    // added by Jia: to enable pipeline execution with pushing-down projection
    virtual SimpleLambda<bool> getProjectionSelection(Handle<Out>& in) = 0;

    // over-ridden by the user so they can supple the actual projection
    virtual SimpleLambda<Handle<Out>> getProjection(Handle<In>& in) = 0;


    // To get aggregation results for queries like TopK
    virtual Handle<Vector<Handle<Out>>>& getAggregatedResults() {
        return aggregationResult;
    }

    // To get aggregation results for queries like TopK
    virtual Handle<Vector<Handle<Out>>>& getAggregatedResultsOptimized() {
        return aggregationResult;
    }


    virtual bool isAggregation() {
        return false;
    }

    // contact from super
    void match(function<void(QueryBase&)> forSelection,
               function<void(QueryBase&)> forSet,
               function<void(QueryBase&)> forQueryOutput) override;

    // get an object that is able to process queries of this type
    SimpleSingleTableQueryProcessorPtr getProcessor();

    // get an object that is able to process filtering of this type
    SimpleSingleTableQueryProcessorPtr getFilterProcessor();

    // get an object that is able to process projection of this type
    SimpleSingleTableQueryProcessorPtr getProjectionProcessor();

    // get an object that is able to process filtering of this type
    BlockQueryProcessorPtr getFilterBlockProcessor();

    // get an object that is able to process projection of this type
    BlockQueryProcessorPtr getProjectionBlockProcessor();


    // gets the number of inputs
    virtual int getNumInputs() override {
        return 1;
    }

    // gets the name of the i^th input type...
    virtual std::string getIthInputType(int i) override {
        if (i == 0)
            return getTypeName<In>();
        else
            return "bad index";
    }

    virtual std::string getQueryType() override {
        return "selection";
    }

protected:
    Handle<Vector<Handle<Out>>> aggregationResult;
};
}

#include "Selection.cc"


#endif
