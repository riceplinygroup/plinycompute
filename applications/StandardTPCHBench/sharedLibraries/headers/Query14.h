#ifndef QUERY14_H
#define QUERY14_H


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

class Q14JoinOut : public Object {

public:

    ENABLE_DEEP_COPY

    double promo_price; //when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0

    double price; //sum(l_extendedprice * (1 - l_discount))

    Q14JoinOut() {}

    Q14JoinOut(TPCHLineItem & lineitem, TPCHPart & part) {

        std::string p_type = part.p_type;
        if (p_type.find("PROMO") == 0) {
            this->promo_price = lineitem.l_extendedprice * (1 - lineitem.l_discount);
        } else {
            this->promo_price = 0;
        }
        this->price = lineitem.l_extendedprice * (1 - lineitem.l_discount);
    }

};




class Q14Join : public JoinComp<Q14JoinOut, TPCHLineItem, TPCHPart> {

public:

    ENABLE_DEEP_COPY

    Q14Join () {}

    Lambda<bool> getSelection (Handle<TPCHLineItem> in1, Handle<TPCHPart> in2) override {
        return makeLambdaFromMember(in1, l_partkey) == makeLambdaFromMember(in2,
           p_partkey);

    }

    Lambda<Handle<Q14JoinOut>> getProjection(Handle<TPCHLineItem> in1, Handle<TPCHPart> in2) override {
        return makeLambda(in1, in2, [](Handle<TPCHLineItem>& in1, Handle<TPCHPart>& in2) {
           Handle<Q14JoinOut> ret = makeObject<Q14JoinOut>(*in1, *in2);
           return ret;
        });
    }


};





class Q14TPCHLineItemSelection : public SelectionComp<TPCHLineItem, TPCHLineItem> {


public:

    ENABLE_DEEP_COPY

    Q14TPCHLineItemSelection () {}


    Lambda<bool> getSelection(Handle<TPCHLineItem> checkMe) override {
        return makeLambda(checkMe,
           [&](Handle<TPCHLineItem>& checkMe) { 
                TPCHLineItem me = *checkMe;
                if ((strcmp(me.l_shipdate.c_str(), "1995-09-01") >= 0) &&
                    (strcmp(me.l_shipdate.c_str(), "1995-10-01") < 0)) {
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

class Q14ValueClass : public Object {

public:

    ENABLE_DEEP_COPY

    double promo_price;

    double price;

    Q14ValueClass () {}

    Q14ValueClass (Q14JoinOut out) {
        this->promo_price = out.promo_price;
        this->price = out.price;
    }

    Q14ValueClass & operator+ (Q14ValueClass & addMe) {
        this->promo_price += addMe.promo_price;
        this->price += addMe.price;
        return *this;
    } 

    double getPromoRevenue () {
        return 100.00 * promo_price / price;
    }

};


class Q14AggOut : public Object {

public:

    ENABLE_DEEP_COPY

    int key;

    Q14ValueClass value;

    Q14AggOut() {}

    int& getKey() {
        return key;
    }

    Q14ValueClass& getValue() {
        return value;
    }

};

class Q14Agg : public AggregateComp<Q14AggOut,
                                               Q14JoinOut,
                                               int,
                                               Q14ValueClass> {

public:

    ENABLE_DEEP_COPY


    Q14Agg () {}

    Lambda<int> getKeyProjection(Handle<Q14JoinOut> aggMe) override {
         return makeLambda(aggMe, [](Handle<Q14JoinOut>& aggMe) { return 0; });
    }

    Lambda<Q14ValueClass> getValueProjection (Handle<Q14JoinOut> aggMe) override {
         return makeLambda(aggMe, [](Handle<Q14JoinOut>& aggMe) {
             Q14ValueClass ret (*aggMe);
             return ret;
         });
    }


};



}

#endif
