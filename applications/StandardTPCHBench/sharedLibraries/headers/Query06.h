#ifndef QUERY06_H
#define QUERY06_H


#include "TPCHSchema.h"
#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "PDBClient.h"
#include "DataTypes.h"
#include "InterfaceFunctions.h"
#include "Handle.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include "QueryOutput.h"
#include "SelectionComp.h"
#include "JoinComp.h"
#include "AggregateComp.h"
#include "DoubleSumResult.h"

using namespace pdb;

namespace tpch {


class Q06TPCHLineItemSelection : public SelectionComp<TPCHLineItem, TPCHLineItem> {


public:

    ENABLE_DEEP_COPY

    Q06TPCHLineItemSelection () {}


    Lambda<bool> getSelection(Handle<TPCHLineItem> checkMe) override {
        return makeLambda(checkMe,
           [&](Handle<TPCHLineItem>& checkMe) { 
                TPCHLineItem me = *checkMe;
                if ((strcmp(me.l_shipdate.c_str(), "1994-01-01") >= 0) &&
                    (strcmp(me.l_shipdate.c_str(), "1995-10-01") < 0) &&
                    (me.l_discount >= 0.05) && (me.l_discount <= 0.07) &&
                    (me.l_quantity < 24)) {
                    return true; 
                } else {
                    return false;
                }
           });
    }

    Lambda<Handle<TPCHLineItem>> getProjection(Handle<TPCHLineItem> checkMe) override {
        return makeLambda(checkMe, [](Handle<TPCHLineItem>& checkMe) { return checkMe; });
    }


};



class Q06Agg : public AggregateComp<DoubleSumResult,
                                               TPCHLineItem,
                                               int,
                                               double> {

public:

    ENABLE_DEEP_COPY


    Q06Agg () {}

    Lambda<int> getKeyProjection(Handle<TPCHLineItem> aggMe) override {
         return makeLambda(aggMe, [](Handle<TPCHLineItem>& aggMe) { return 0; });
    }

    Lambda<double> getValueProjection (Handle<TPCHLineItem> aggMe) override {
         return makeLambda(aggMe, [](Handle<TPCHLineItem>& aggMe) {
             return aggMe->l_extendedprice * aggMe->l_discount;
         });
    }


};



}

#endif
