#ifndef QUERY04_H
#define QUERY04_H


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


using namespace pdb;

namespace tpch {

class Q04TPCHOrderSelection : public SelectionComp<TPCHOrder, TPCHOrder> {


public:

    ENABLE_DEEP_COPY

    Q04TPCHOrderSelection () {}


    Lambda<bool> getSelection(Handle<TPCHOrder> checkMe) override {
        return makeLambda(checkMe,
           [&](Handle<TPCHOrder>& checkMe) { 

                if ((strcmp(checkMe->o_orderdate.c_str(), "1993-07-01") >= 0) &&
                    (strcmp(checkMe->o_orderdate.c_str(), "1993-10-01") < 0)) {
                    return true; 
                } else {
                    return false;
                }
           });
    }

    Lambda<Handle<TPCHOrder>> getProjection(Handle<TPCHOrder> checkMe) override {
        return makeLambda(checkMe, [](Handle<TPCHOrder>& checkMe) { return checkMe; });
    }


};


class Q04Join : public JoinComp<TPCHOrder, TPCHOrder, TPCHLineItem> {

public:

    ENABLE_DEEP_COPY

    Q04Join () {}

    Lambda<bool> getSelection (Handle<TPCHOrder> in1, Handle<TPCHLineItem> in2) override {
        return makeLambdaFromMember(in1, o_orderkey) == makeLambdaFromMember(in2,
           l_orderkey);
         
    }
    
    Lambda<Handle<TPCHOrder>> getProjection(Handle<TPCHOrder> in1, Handle<TPCHLineItem> in2) override {
        return makeLambda(in1, in2, [](Handle<TPCHOrder>& in1, Handle<TPCHLineItem>& in2) { return in1; });
    }


};

class Q04AggOut : public Object {

public:

    ENABLE_DEEP_COPY

    String o_orderpriority;
    int count;

    Q04AggOut () {}

    String & getKey() {
        return o_orderpriority;
    }

    int & getValue() {
        return count;
    }

};



class Q04Agg : public AggregateComp<Q04AggOut,
                                               TPCHOrder,
                                               String,
                                               int> {

public:

    ENABLE_DEEP_COPY


    Q04Agg () {}

    Lambda<String> getKeyProjection(Handle<TPCHOrder> aggMe) override {
         return makeLambdaFromMember(aggMe, o_orderpriority);
    }

    Lambda<int> getValueProjection (Handle<TPCHOrder> aggMe) override {
         return makeLambda(aggMe, [](Handle<TPCHOrder>& aggMe) {
             return 1;
         });
    }


};



}

#endif
