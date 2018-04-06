#ifndef QUERY01_H
#define QUERY01_H


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

class Q01KeyClass : public Object {

public:

   String l_returnflag;
   String l_linestatus;


    ENABLE_DEEP_COPY

    Q01KeyClass () {}

    Q01KeyClass (std::string l_returnflag, std::string l_linestatus) {
        this->l_returnflag = l_returnflag;
        this->l_linestatus = l_linestatus;
    }

    size_t hash() const {
        return String((std::string(l_returnflag)+std::string(l_linestatus))).hash();
    }

    bool operator==(const Q01KeyClass& toMe) {
        if ((l_returnflag == toMe.l_returnflag) && (l_linestatus == toMe.l_linestatus)) {
            return true;
        } else {
            return false;
        }
    }    

};


class Q01ValueClass : public Object {

public:

    double sum_qty;

    double sum_base_price;

    double sum_disc_price;

    double sum_charge;

    double sum_disc;

    long count;

    ENABLE_DEEP_COPY

    Q01ValueClass () {}

    Q01ValueClass (TPCHLineItem & lineitem) {
        this->sum_qty = lineitem.l_quantity;
        this->sum_base_price = lineitem.l_extendedprice;
        this->sum_disc_price = lineitem.l_extendedprice * (1 - lineitem.l_discount);
        this->sum_charge = lineitem.l_extendedprice * (1 - lineitem.l_discount) * (1 + lineitem.l_tax);
        this->sum_disc = lineitem.l_discount;
        this->count = 1;
    }

    double getAvgQty () {
        return sum_qty / (double)(count);
    }

    double getAvgPrice () {
        return sum_base_price / (double)(count);
    }

    double getAvgDiscount () {
        return sum_disc / (double)(count);
    }

    Q01ValueClass & operator+ (const Q01ValueClass & rhs) {
        this->sum_qty += rhs.sum_qty;
        this->sum_base_price += rhs.sum_base_price;
        this->sum_disc_price += rhs.sum_disc_price;
        this->sum_charge += rhs.sum_charge;
        this->sum_disc += rhs.sum_disc;
        this->count += rhs.count;
        return *this;
    }

};

class Q01AggOut : public Object {

public:

    Q01KeyClass key;

    Q01ValueClass value;

    ENABLE_DEEP_COPY

    Q01AggOut() {};

    Q01KeyClass & getKey() {
       return this->key;
    }

    Q01ValueClass & getValue() {
       return this->value;
    }

};



class Q01Agg : public AggregateComp<Q01AggOut,
                                               TPCHLineItem,
                                               Q01KeyClass,
                                               Q01ValueClass> {

public:

    ENABLE_DEEP_COPY


    Q01Agg () {}

    Lambda<Q01KeyClass> getKeyProjection(Handle<TPCHLineItem> aggMe) override {
         return makeLambda(aggMe, [](Handle<TPCHLineItem>& aggMe) {
              TPCHLineItem me = *aggMe;
              Q01KeyClass key (me.l_returnflag, me.l_linestatus);
              return key;
         });
    }

    Lambda<Q01ValueClass> getValueProjection (Handle<TPCHLineItem> aggMe) override {
         return makeLambda(aggMe, [](Handle<TPCHLineItem>& aggMe) {
             Q01ValueClass ret(*aggMe);
             return ret;
         });
    }

};

}

#endif
