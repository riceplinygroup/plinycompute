#ifndef QUERY12_H
#define QUERY12_H


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


class Q12TPCHLineItemSelection : public SelectionComp<TPCHLineItem, TPCHLineItem> {


public:

    ENABLE_DEEP_COPY

    Q12TPCHLineItemSelection () {}

    Lambda<bool> getSelection(Handle<TPCHLineItem> checkMe) override {
        return makeLambda(checkMe,
           [&](Handle<TPCHLineItem>& checkMe) { 
                TPCHLineItem me = *checkMe;
                if (((strcmp(me.l_shipmode.c_str(), "MAIL") == 0) || 
                     (strcmp(me.l_shipmode.c_str(), "SHIP") == 0)) &&
                    (strcmp(me.l_commitdate.c_str(), me.l_receiptdate.c_str()) < 0) &&
                    (strcmp(me.l_shipdate.c_str(), me.l_commitdate.c_str()) < 0) &&
                    (strcmp(me.l_receiptdate.c_str(), "1994-01-01") >= 0) &&
                    (strcmp(me.l_receiptdate.c_str(), "1995-01-01") < 0)) {
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


class Q12JoinOut : public Object {

public:

    ENABLE_DEEP_COPY

    String l_shipmode;
    int high_line_count;
    int low_line_count;

    Q12JoinOut () {}

    Q12JoinOut (TPCHOrder order, TPCHLineItem lineItem) {
        this->l_shipmode = lineItem.l_shipmode;
        if ((order.o_orderpriority == "1-URGENT") || 
          (order.o_orderpriority == "2-HIGH")) {
            this->high_line_count = 1;
        } else {
            this->high_line_count = 0;
        }
        if ((order.o_orderpriority != "1-URGENT") &&
          (order.o_orderpriority != "2-HIGH")) {
            this->low_line_count = 1;
        } else {
            this->low_line_count = 0;
        }
    }
    

};


class Q12Join : public JoinComp<Q12JoinOut, TPCHOrder, TPCHLineItem> {

public:

    ENABLE_DEEP_COPY

    Q12Join () {}

    Lambda<bool> getSelection (Handle<TPCHOrder> in1, Handle<TPCHLineItem> in2) override {
        return makeLambdaFromMember(in1, o_orderkey) == makeLambdaFromMember(in2,
           l_orderkey);
         
    }
    
    Lambda<Handle<Q12JoinOut>> getProjection(Handle<TPCHOrder> in1, Handle<TPCHLineItem> in2) override {
        return makeLambda(in1, in2, [](Handle<TPCHOrder>& in1, Handle<TPCHLineItem>& in2) { 
           Handle<Q12JoinOut> ret = makeObject<Q12JoinOut>(*in1, *in2);
           return ret; 
        });
    }


};


class Q12ValueClass : public Object {

public:

    ENABLE_DEEP_COPY

    int high_line_count;
    int low_line_count;

    Q12ValueClass () {}

    Q12ValueClass (int high, int low) {
       this->high_line_count = high;
       this->low_line_count = low;
    }

    Q12ValueClass & operator+ (Q12ValueClass& addMe) {
       this->high_line_count += addMe.high_line_count;
       this->low_line_count += addMe.low_line_count;
       return *this;
    }

};



class Q12AggOut : public Object {

public:

    ENABLE_DEEP_COPY

    String l_shipmode;
    Q12ValueClass value;

    Q12AggOut () {}

    String & getKey() {
        return l_shipmode;
    }

    Q12ValueClass & getValue() {
        return value;
    }

};



class Q12Agg : public AggregateComp<Q12AggOut,
                                               Q12JoinOut,
                                               String,
                                               Q12ValueClass> {

public:

    ENABLE_DEEP_COPY


    Q12Agg () {}

    Lambda<String> getKeyProjection(Handle<Q12JoinOut> aggMe) override {
         return makeLambdaFromMember(aggMe, l_shipmode);
    }

    Lambda<Q12ValueClass> getValueProjection (Handle<Q12JoinOut> aggMe) override {
         return makeLambda(aggMe, [](Handle<Q12JoinOut>& aggMe) {
             Q12ValueClass ret (aggMe->high_line_count, aggMe->low_line_count);
             return ret;
         });
    }


};



}

#endif
