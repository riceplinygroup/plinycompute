#ifndef QUERY17_H
#define QUERY17_H


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
#include "Avg.h"
#include "DoubleSumResult.h"
#include "AvgResult.h"

using namespace pdb;

namespace tpch {


class Q17TPCHPartSelection : public SelectionComp<TPCHPart, TPCHPart> {

private:

    String p_brand;
    String p_container;

public:

    ENABLE_DEEP_COPY

    Q17TPCHPartSelection () {}

    Q17TPCHPartSelection (std::string p_brand, std::string p_container) {
       this->p_brand = p_brand;
       this->p_container = p_container;
    }

    Lambda<bool> getSelection(Handle<TPCHPart> checkMe) override {
        return makeLambda(checkMe,
           [&](Handle<TPCHPart>& checkMe) {
                if ((checkMe->p_brand == p_brand) && 
                      (checkMe->p_container == p_container)) {
                    return true;
                } else {
                    return false;
                }
           });
    }

    Lambda<Handle<TPCHPart>> getProjection(Handle<TPCHPart> checkMe) override {
        return makeLambda(checkMe, [](Handle<TPCHPart>& checkMe) { return checkMe; });
    }


}; 


class Q17JoinedTPCHPartTPCHLineItem : public Object {

public:

    ENABLE_DEEP_COPY

    Q17JoinedTPCHPartTPCHLineItem () {}

    Q17JoinedTPCHPartTPCHLineItem (TPCHPart& part, TPCHLineItem& lineItem) {

        this->p_brand = part.p_brand;
        this->p_container = part.p_container;
        this->l_partkey = lineItem.l_partkey;
        this->l_extendedprice = lineItem.l_extendedprice;
        this->l_quantity = lineItem.l_quantity;

    }
    

    String p_brand;

    String p_container;

    int l_partkey;

    double l_extendedprice;

    double l_quantity;


};



class Q17TPCHPartTPCHLineItemJoin : public JoinComp<Q17JoinedTPCHPartTPCHLineItem, TPCHPart, TPCHLineItem> {

public:

    ENABLE_DEEP_COPY

    Q17TPCHPartTPCHLineItemJoin () {}

    Lambda<bool> getSelection (Handle<TPCHPart> in1, Handle<TPCHLineItem> in2) override {
        return makeLambdaFromMember(in1, p_partkey) == makeLambdaFromMember(in2,
           l_partkey);

    }

    Lambda<Handle<Q17JoinedTPCHPartTPCHLineItem>> getProjection(Handle<TPCHPart> in1, Handle<TPCHLineItem> in2) override {
        return makeLambda(in1, in2, [](Handle<TPCHPart>& in1, Handle<TPCHLineItem>& in2) {
            Handle<Q17JoinedTPCHPartTPCHLineItem> ret = makeObject<Q17JoinedTPCHPartTPCHLineItem>(*in1, *in2);
            return ret;
        });
    }


};


class Q17TPCHPartTPCHLineItemIdentitySelection : public SelectionComp<Q17JoinedTPCHPartTPCHLineItem, Q17JoinedTPCHPartTPCHLineItem> {

public:

    ENABLE_DEEP_COPY

    Q17TPCHPartTPCHLineItemIdentitySelection () {}

    Lambda<bool> getSelection (Handle<Q17JoinedTPCHPartTPCHLineItem> checkMe) override {
        return makeLambda(checkMe, [](Handle<Q17JoinedTPCHPartTPCHLineItem> & checkMe) {
             return true;
         });
    }

    Lambda<Handle<Q17JoinedTPCHPartTPCHLineItem>> getProjection (Handle<Q17JoinedTPCHPartTPCHLineItem> checkMe) override {
        return makeLambda(checkMe,  [](Handle<Q17JoinedTPCHPartTPCHLineItem> & checkMe) {
             return checkMe;
        });
    }

};



class Q17TPCHLineItemAvgQuantity : public AggregateComp<AvgResult,
                                               Q17JoinedTPCHPartTPCHLineItem,
                                               int,
                                               Avg> {

public:

    ENABLE_DEEP_COPY


    Q17TPCHLineItemAvgQuantity () {}

    Lambda<int> getKeyProjection(Handle<Q17JoinedTPCHPartTPCHLineItem> aggMe) override {
         return makeLambdaFromMember(aggMe, l_partkey);
    }

    Lambda<Avg> getValueProjection (Handle<Q17JoinedTPCHPartTPCHLineItem> aggMe) override {
         return makeLambda(aggMe, [](Handle<Q17JoinedTPCHPartTPCHLineItem>& aggMe) {
             Avg ret(0.2 * aggMe->l_quantity, 1);
             return ret;
         });
    }


};


class Q17TPCHPartTPCHLineItemAvgJoin : public JoinComp<double, Q17JoinedTPCHPartTPCHLineItem, AvgResult> {

public:

    ENABLE_DEEP_COPY

    Q17TPCHPartTPCHLineItemAvgJoin () {}

    Lambda<bool> getSelection (Handle<Q17JoinedTPCHPartTPCHLineItem> in1, Handle<AvgResult> in2) override {
        return makeLambdaFromMember(in1, l_partkey) == makeLambdaFromMember(in2,
           key) && makeLambda(in1, in2, [](Handle<Q17JoinedTPCHPartTPCHLineItem>& in1, Handle<AvgResult>& in2) {
                   if (in1->l_quantity < in2->getAvg()) {
                       return true;
                   } else {
                       return false;
                   }
               });

    }

    Lambda<Handle<double>> getProjection(Handle<Q17JoinedTPCHPartTPCHLineItem> in1, Handle<AvgResult> in2) override {
        return makeLambda(in1, in2, [](Handle<Q17JoinedTPCHPartTPCHLineItem>& in1, Handle<AvgResult>& in2) {
            Handle<double> ret = makeObject<double>(in1->l_extendedprice);
            return ret;
        });
    }


};


class Q17PriceSum : public AggregateComp<DoubleSumResult,
                                               double,
                                               int,
                                               double> {

public:

    ENABLE_DEEP_COPY


    Q17PriceSum () {}

    Lambda<int> getKeyProjection(Handle<double> aggMe) override {
         return makeLambda(aggMe, [](Handle<double> & aggMe) {
              return 0;
          });
    }

    Lambda<double> getValueProjection (Handle<double> aggMe) override {
         return makeLambda(aggMe, [](Handle<double>& aggMe) {
             return *aggMe;
         });
    }


};
}

#endif
