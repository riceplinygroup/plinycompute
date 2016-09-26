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
#ifndef PDB_QUERYINTERMEDIARYREP_SELECTION_H
#define PDB_QUERYINTERMEDIARYREP_SELECTION_H

#include "Handle.h"
#include "RecordPredicateIr.h"
#include "SetExpressionIr.h"
#include "SimpleSingleTableQueryProcessor.h"

using pdb::Handle;
using pdb::SimpleSingleTableQueryProcessorPtr;

namespace pdb_detail
{
    /**
     * An operation that produces a subset of a given set of "input" records by applying a given boolean condition to
     * each record of the input set. Only those input records that that pass the given condition are retained in the
     * produced subset.
     */
    class SelectionIr : public SetExpressionIr
    {
    public:

        /**
         * Creates a selection over the given input set filtered by the given condition.
         *
         * Adds the returned selection to inputSet's consumers.
         *
         * @param inputSet the source of records from which the selection subset will be drawn.
         * @param condition the filter to apply to the input set to produce the subset
         * @param the page processor originally associated with user query node
         * @return the selection
         */
        static Handle<SelectionIr> make(Handle<SetExpressionIr> inputSet, Handle<RecordPredicateIr> condition,
                                        SimpleSingleTableQueryProcessorPtr pageProcessor);

        /**
         * Creates a selection over the given input set filtered by the given condition.
         *
         * @param inputSet the source of records from which the selection subset will be drawn.
         * @param condition the filter to apply to the input set to produce the subset
         * @param the page processor originally associated with user query node
         * @return the selection
         */
        SelectionIr(Handle<SetExpressionIr> inputSet, Handle<RecordPredicateIr> condition, SimpleSingleTableQueryProcessorPtr pageProcessor);

        // contract from super
        virtual void execute(QueryNodeIrAlgo &algo) override;

        // contract from super
        virtual void execute(SetExpressionIrAlgo &algo) override;

        /**
         * @return the set of input records from which the selection subset is drawn.
         */
        virtual Handle<SetExpressionIr> getInputSet();

        /**
         * @return the filter applied to each record of the input set to determine membership in the subset.
         */
        virtual Handle<RecordPredicateIr> getCondition();

        /**
         * @return gets a processor capable of applying this node type to input pages to produce output pages.
         */
        SimpleSingleTableQueryProcessorPtr getPageProcessor();

    private:

        /**
         * Records source.
         */
        Handle<SetExpressionIr> _inputSet;

        /**
         * Records filter.
         */
        Handle<RecordPredicateIr> _condition;

        SimpleSingleTableQueryProcessorPtr _pageProcessor;
    };
}

#endif //PDB_QUERYINTERMEDIARYREP_SELECTION_H
