#ifndef QUERY03_H
#define QUERY03_H


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

class Q03TPCHCustomerSelection : public SelectionComp<TPCHCustomer, TPCHCustomer> {

private:

   String segment;

public:

    ENABLE_DEEP_COPY

    Q03TPCHCustomerSelection () {}

    Q03TPCHCustomerSelection (std::string segment) {
        this->segment = segment;
    }

    Lambda<bool> getSelection(Handle<TPCHCustomer> checkMe) override {
        return makeLambda(checkMe,
           [&](Handle<TPCHCustomer>& checkMe) { 
               if (checkMe->c_mktsegment == this->segment) {
                   return true;
               } else {
                   return false;
               } 
           });
    }

    Lambda<Handle<TPCHCustomer>> getProjection(Handle<TPCHCustomer> checkMe) override {
        return makeLambda(checkMe, [](Handle<TPCHCustomer>& checkMe) { 
            return checkMe; 
        });
    }


};


class Q03TPCHOrderSelection : public SelectionComp<TPCHOrder, TPCHOrder> {

private:

   String date;

public:

    ENABLE_DEEP_COPY

    Q03TPCHOrderSelection () {}

    Q03TPCHOrderSelection (std::string date) {
        this->date = date;
    }

    Lambda<bool> getSelection(Handle<TPCHOrder> checkMe) override {
        return makeLambda(checkMe,
           [&](Handle<TPCHOrder>& checkMe) { return (strcmp(checkMe->o_orderdate.c_str(), this->date.c_str()) < 0); });
    }

    Lambda<Handle<TPCHOrder>> getProjection(Handle<TPCHOrder> checkMe) override {
        return makeLambda(checkMe, [](Handle<TPCHOrder>& checkMe) { 
                 return checkMe; 
        });
    }


};


class Q03TPCHLineItemSelection : public SelectionComp<TPCHLineItem, TPCHLineItem> {

private:

   String date;

public:

    ENABLE_DEEP_COPY

    Q03TPCHLineItemSelection () {}

    Q03TPCHLineItemSelection (std::string date) {
        this->date = date;
    }

    Lambda<bool> getSelection(Handle<TPCHLineItem> checkMe) override {
        return makeLambda(checkMe,
           [&](Handle<TPCHLineItem>& checkMe) { return (strcmp(checkMe->l_shipdate.c_str(), this->date.c_str()) > 0); });
    }

    Lambda<Handle<TPCHLineItem>> getProjection(Handle<TPCHLineItem> checkMe) override {
        return makeLambda(checkMe, [](Handle<TPCHLineItem>& checkMe) { 
           return checkMe; 
        });
    }


};


class Q03JoinOut : public Object {

public:

    ENABLE_DEEP_COPY

    int l_orderkey;
    double revenue;
    String o_orderdate;
    int o_shippriority;

    Q03JoinOut () {}

    Q03JoinOut (TPCHLineItem& lineitem, TPCHOrder& order) {
        this->l_orderkey = lineitem.l_orderkey;
        this->o_orderdate = order.o_orderdate;
        this->o_shippriority = order.o_shippriority;
        this->revenue = lineitem.l_extendedprice * (1 - lineitem.l_discount);
    }    


};

class Q03Join : public JoinComp<Q03JoinOut, TPCHCustomer, TPCHOrder, TPCHLineItem> {

public:

    ENABLE_DEEP_COPY

    Q03Join () {}

    Lambda<bool> getSelection (Handle<TPCHCustomer> in1, Handle<TPCHOrder> in2, Handle<TPCHLineItem> in3) override {
        return makeLambdaFromMember(in1, c_custkey) == makeLambdaFromMember(in2,
           o_custkey) && makeLambdaFromMember(in2, o_orderkey) == makeLambdaFromMember(in3,
           l_orderkey);
         
    }
    
    Lambda<Handle<Q03JoinOut>> getProjection(Handle<TPCHCustomer> in1, Handle<TPCHOrder> in2, Handle<TPCHLineItem> in3) override {
        return makeLambda(in1, in2, in3, [](Handle<TPCHCustomer>& in1, Handle<TPCHOrder>& in2, Handle<TPCHLineItem>& in3) {  
             Handle<Q03JoinOut> ret = makeObject<Q03JoinOut>(*in3, *in2);
             return ret;
         });
    }


};


class Q03KeyClass : public Object {

public:

    ENABLE_DEEP_COPY

    int l_orderkey;
    String o_orderdate;
    int o_shippriority;

    Q03KeyClass () {}

    Q03KeyClass (Q03JoinOut & joinOut) {
        this->l_orderkey = joinOut.l_orderkey;
        this->o_orderdate = joinOut.o_orderdate;
        this->o_shippriority = joinOut.o_shippriority;
    }

    size_t hash() const {
        return l_orderkey;
    }

    bool operator==(const Q03KeyClass& toMe) {
        if ((l_orderkey == toMe.l_orderkey) && (o_orderdate == toMe.o_orderdate)
              &&(o_shippriority == toMe.o_shippriority)) {
            return true;
        } else {
            return false;
        }
    }

};


class Q03AggOut : public Object {

public:

    ENABLE_DEEP_COPY

    Q03KeyClass key;
    double value;

    Q03AggOut () {}

    Q03KeyClass & getKey() {
        return key;
    }

    double & getValue() {
        return value;
    }

};


class Q03Agg : public AggregateComp<Q03AggOut,
                                               Q03JoinOut,
                                               Q03KeyClass,
                                               double> {

public:

    ENABLE_DEEP_COPY


    Q03Agg () {}

    Lambda<Q03KeyClass> getKeyProjection(Handle<Q03JoinOut> aggMe) override {
         return makeLambda(aggMe, [](Handle<Q03JoinOut>& checkMe) { 
             Q03KeyClass ret(*checkMe);
             return ret;
         });
    }

    Lambda<double> getValueProjection (Handle<Q03JoinOut> aggMe) override {
         return makeLambdaFromMember(aggMe, revenue);
    }


};



}

#endif
