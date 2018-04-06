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
#ifndef QUERY02_H
#define QUERY02_H


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

class Q02TPCHRegionSelection : public SelectionComp<TPCHRegion, TPCHRegion> {

private:

   String r_name;

public:

    ENABLE_DEEP_COPY

    Q02TPCHRegionSelection () {}

    Q02TPCHRegionSelection (std::string r_name) {
        this->r_name = r_name;
    }

    Lambda<bool> getSelection(Handle<TPCHRegion> checkMe) override {
        return makeLambdaFromMember(checkMe, r_name ) == makeLambda(checkMe,
           [&](Handle<TPCHRegion>& checkMe) { return this->r_name; });
    }

    Lambda<Handle<TPCHRegion>> getProjection(Handle<TPCHRegion> checkMe) override {
        return makeLambda(checkMe, [](Handle<TPCHRegion>& checkMe) { return checkMe; });
    }


};



class Q02TPCHNationJoin : public JoinComp<TPCHNation, TPCHRegion, TPCHNation> {

public:

    ENABLE_DEEP_COPY

    Q02TPCHNationJoin () {}

    Lambda<bool> getSelection (Handle<TPCHRegion> in1, Handle<TPCHNation> in2) override {
        return makeLambdaFromMember(in1, r_regionkey) == makeLambdaFromMember(in2,
           n_regionkey);
         
    }
    
    Lambda<Handle<TPCHNation>> getProjection(Handle<TPCHRegion> in1, Handle<TPCHNation> in2) override {
        return makeLambda(in1, in2, [](Handle<TPCHRegion>& in1, Handle<TPCHNation>& in2) { return in2; });
    }


};


class Q02TPCHSupplierJoinOutput : public Object {

public:

    ENABLE_DEEP_COPY

    String n_name;
    int s_suppkey;
    double s_acctbal;
    String s_name;
    String s_address;
    String s_phone;
    String s_comment;

    Q02TPCHSupplierJoinOutput () {}

    Q02TPCHSupplierJoinOutput (String & n_name, TPCHSupplier & supplier) {
        this->n_name = n_name;
        this->s_suppkey = supplier.s_suppkey;
        this->s_acctbal = supplier.s_acctbal;
        this->s_name = supplier.s_name;
        this->s_address = supplier.s_address;
        this->s_phone = supplier.s_phone;
        this->s_comment = supplier.s_comment;
    }

};



class Q02TPCHSupplierJoin : public JoinComp<Q02TPCHSupplierJoinOutput, TPCHNation, TPCHSupplier> {

public:

    ENABLE_DEEP_COPY

    Q02TPCHSupplierJoin () {}

    Lambda<bool> getSelection (Handle<TPCHNation> in1, Handle<TPCHSupplier> in2) override {
        return makeLambdaFromMember(in1, n_nationkey) == makeLambdaFromMember(in2,
           s_nationkey);

    }

    Lambda<Handle<Q02TPCHSupplierJoinOutput>> getProjection(Handle<TPCHNation> in1, Handle<TPCHSupplier> in2) override {
        return makeLambda(in1, in2, [](Handle<TPCHNation>& in1, Handle<TPCHSupplier>& in2) { 

             Handle<Q02TPCHSupplierJoinOutput> ret = makeObject<Q02TPCHSupplierJoinOutput> (in1->n_name, *in2);
             return ret;

        });
    
}


};

class Q02TPCHTPCHPartSuppJoinOutput : public Object {

public:

    ENABLE_DEEP_COPY

    String n_name;
    double s_acctbal;
    String s_name;
    String s_address;
    String s_phone;
    String s_comment;
    int ps_partkey;
    double ps_supplycost;

    Q02TPCHTPCHPartSuppJoinOutput () {}

    Q02TPCHTPCHPartSuppJoinOutput (Q02TPCHSupplierJoinOutput& supplier, TPCHTPCHPartSupp& ps) {
        this->n_name = supplier.n_name;
        this->s_acctbal = supplier.s_acctbal;
        this->s_name = supplier.s_name;
        this->s_address = supplier.s_address;
        this->s_phone = supplier.s_phone;
        this->s_comment = supplier.s_comment;
        this->ps_partkey = ps.ps_partkey;
        this->ps_supplycost = ps.ps_supplycost;
    }    

};

class Q02TPCHTPCHPartSuppJoin : public JoinComp<Q02TPCHTPCHPartSuppJoinOutput, Q02TPCHSupplierJoinOutput, TPCHTPCHPartSupp> {

public:

    ENABLE_DEEP_COPY

    Q02TPCHTPCHPartSuppJoin () {}

    Lambda<bool> getSelection (Handle<Q02TPCHSupplierJoinOutput> in1, Handle<TPCHTPCHPartSupp> in2) override {
        return makeLambdaFromMember(in1, s_suppkey) == makeLambdaFromMember(in2,
           ps_suppkey);

    }

    Lambda<Handle<Q02TPCHTPCHPartSuppJoinOutput>> getProjection(Handle<Q02TPCHSupplierJoinOutput> in1, Handle<TPCHTPCHPartSupp> in2) override {
        return makeLambda(in1, in2, [](Handle<Q02TPCHSupplierJoinOutput>& in1, Handle<TPCHTPCHPartSupp>& in2) { 
            Handle<Q02TPCHTPCHPartSuppJoinOutput> ret = makeObject<Q02TPCHTPCHPartSuppJoinOutput> (*in1, *in2);
            return ret; 
        });
    }

};



class Q02TPCHPartSelection : public SelectionComp<TPCHPart, TPCHPart> {

private:
    int size;
    String type;

public:

    ENABLE_DEEP_COPY

    Q02TPCHPartSelection () {}

    Q02TPCHPartSelection (int size, std::string type) {
        this->size = size;
        this->type = type;
    }

    Lambda<bool> getSelection(Handle<TPCHPart> checkMe) override {
        return makeLambda(checkMe,
           [&](Handle<TPCHPart>& checkMe) { return (checkMe->p_size == this->size); }) && makeLambda(checkMe,
           [&](Handle<TPCHPart>& checkMe) { 
               return checkMe->p_type.endsWith(this->type);
               
           });
    }

    Lambda<Handle<TPCHPart>> getProjection(Handle<TPCHPart> checkMe) override {
        return makeLambda(checkMe, [](Handle<TPCHPart>& checkMe) { return checkMe; });
    }


};



class Q02TPCHPartJoinOutput : public Object {

public:
    
    ENABLE_DEEP_COPY
    
    int p_partkey;
    String p_mfgr;
    String n_name;
    double s_acctbal;
    String s_name;
    String s_address;
    String s_phone;
    String s_comment;
    double ps_supplycost;

    Q02TPCHPartJoinOutput () {}

    Q02TPCHPartJoinOutput (TPCHPart& part, Q02TPCHTPCHPartSuppJoinOutput& supplier) {
        this->n_name = supplier.n_name;
        this->s_acctbal = supplier.s_acctbal;
        this->s_name = supplier.s_name;
        this->s_address = supplier.s_address;
        this->s_phone = supplier.s_phone;
        this->s_comment = supplier.s_comment;
        this->ps_supplycost = supplier.ps_supplycost;
        this->p_partkey = part.p_partkey;
        this->p_mfgr = part.p_mfgr;
    }

};


class Q02TPCHPartJoin : public JoinComp<Q02TPCHPartJoinOutput, TPCHPart, Q02TPCHTPCHPartSuppJoinOutput> {

public:

    ENABLE_DEEP_COPY

    Q02TPCHPartJoin () {}

    Lambda<bool> getSelection (Handle<TPCHPart> in1, Handle<Q02TPCHTPCHPartSuppJoinOutput> in2) override {
        return makeLambdaFromMember(in1, p_partkey) == makeLambdaFromMember(in2,
           ps_partkey);

    }

    Lambda<Handle<Q02TPCHPartJoinOutput>> getProjection(Handle<TPCHPart> in1, Handle<Q02TPCHTPCHPartSuppJoinOutput> in2) override {
        return makeLambda(in1, in2, [](Handle<TPCHPart>& in1, Handle<Q02TPCHTPCHPartSuppJoinOutput>& in2) { 
             Handle<Q02TPCHPartJoinOutput> ret = makeObject<Q02TPCHPartJoinOutput> (*in1, *in2);
             return ret;
        });
    }

};

class Q02TPCHPartJoinOutputIdentitySelection : public SelectionComp<Q02TPCHPartJoinOutput, Q02TPCHPartJoinOutput> {

public:

    ENABLE_DEEP_COPY

    Q02TPCHPartJoinOutputIdentitySelection () {}

    Lambda<bool> getSelection (Handle<Q02TPCHPartJoinOutput> checkMe) override {
        return makeLambda(checkMe, [](Handle<Q02TPCHPartJoinOutput> & checkMe) {
             return true;
         });
    }

    Lambda<Handle<Q02TPCHPartJoinOutput>> getProjection (Handle<Q02TPCHPartJoinOutput> checkMe) override {
        return makeLambda(checkMe,  [](Handle<Q02TPCHPartJoinOutput> & checkMe) {
             return checkMe;
        });
    }

};




class MinDouble : public Object {

public:

    double min;

    ENABLE_DEEP_COPY

    MinDouble () {}

    void setValue (double min) {
        this->min = min;
    }

    MinDouble& operator+ (MinDouble& addMeIn) {
        if(addMeIn.min < min) {
            min = addMeIn.min;
        }
        return *this;
    }

};


class Q02MinCostPerTPCHPart : public Object {

public:

    int ps_partkey;
    MinDouble min;

    ENABLE_DEEP_COPY

    Q02MinCostPerTPCHPart () {}

    int & getKey() {
        return ps_partkey;
    }

    MinDouble & getValue() {
        return min;
    }

};


class Q02MinAgg : public AggregateComp<Q02MinCostPerTPCHPart,
                                               Q02TPCHPartJoinOutput,
                                               int,
                                               MinDouble> {

public:

    ENABLE_DEEP_COPY


    Q02MinAgg () {}

    Lambda<int> getKeyProjection(Handle<Q02TPCHPartJoinOutput> aggMe) override {
         return makeLambdaFromMember(aggMe, p_partkey);
    }

    Lambda<MinDouble> getValueProjection (Handle<Q02TPCHPartJoinOutput> aggMe) override {
         return makeLambda(aggMe, [](Handle<Q02TPCHPartJoinOutput>& aggMe) {
             MinDouble ret;
             ret.setValue (aggMe->ps_supplycost);
             return ret;
         });
    }


};


class Q02MinCostJoinOutput : public Object {

public:

    ENABLE_DEEP_COPY

    int p_partkey;
    String p_mfgr;
    String n_name;
    double s_acctbal;
    String s_name;
    String s_address;
    String s_phone;
    String s_comment;
    double ps_supplycost;
    double min;

    Q02MinCostJoinOutput () {}

    Q02MinCostJoinOutput (Q02MinCostPerTPCHPart& minCost, Q02TPCHPartJoinOutput& supplier) {
        this->n_name = supplier.n_name;
        this->s_acctbal = supplier.s_acctbal;
        this->s_name = supplier.s_name;
        this->s_address = supplier.s_address;
        this->s_phone = supplier.s_phone;
        this->s_comment = supplier.s_comment;
        this->p_partkey = supplier.p_partkey;
        this->p_mfgr = supplier.p_mfgr;
        this->ps_supplycost = supplier.ps_supplycost;
        this->min = minCost.min.min;
    }

};




class Q02MinCostJoin : public JoinComp<Q02MinCostJoinOutput, Q02MinCostPerTPCHPart, Q02TPCHPartJoinOutput> {

public:

    ENABLE_DEEP_COPY


    Q02MinCostJoin () {}

    Lambda<bool> getSelection (Handle<Q02MinCostPerTPCHPart> in1, Handle<Q02TPCHPartJoinOutput> in2) override {
        return makeLambdaFromMember(in1, ps_partkey) == makeLambdaFromMember(in2, p_partkey);
    }

    Lambda<Handle<Q02MinCostJoinOutput>> getProjection(Handle<Q02MinCostPerTPCHPart> in1, Handle<Q02TPCHPartJoinOutput> in2) override {
        return makeLambda(in1, in2, [](Handle<Q02MinCostPerTPCHPart>& in1, Handle<Q02TPCHPartJoinOutput>& in2) {
             Handle<Q02MinCostJoinOutput> ret = makeObject<Q02MinCostJoinOutput> (*in1, *in2);
             return ret;
        });
    }

};


class Q02MinCostSelectionOutput : public Object {

public:

    ENABLE_DEEP_COPY

    int p_partkey;
    String p_mfgr;
    String n_name;
    double s_acctbal;
    String s_name;
    String s_address;
    String s_phone;
    String s_comment;

    Q02MinCostSelectionOutput () {}

    Q02MinCostSelectionOutput (Q02MinCostJoinOutput& minCost) {
        this->n_name = minCost.n_name;
        this->s_acctbal = minCost.s_acctbal;
        this->s_name = minCost.s_name;
        this->s_address = minCost.s_address;
        this->s_phone = minCost.s_phone;
        this->s_comment = minCost.s_comment;
        this->p_partkey = minCost.p_partkey;
        this->p_mfgr = minCost.p_mfgr;
    }

};



class Q02MinCostSelection : public SelectionComp<Q02MinCostSelectionOutput, Q02MinCostJoinOutput> {


public:

    ENABLE_DEEP_COPY

    Q02MinCostSelection () {}

    Lambda<bool> getSelection(Handle<Q02MinCostJoinOutput> checkMe) override {
        return makeLambdaFromMember(checkMe, ps_supplycost) == 
                  makeLambdaFromMember(checkMe, min);
    }

    Lambda<Handle<Q02MinCostSelectionOutput>> getProjection(Handle<Q02MinCostJoinOutput> checkMe) override {
        return makeLambda(checkMe, [](Handle<Q02MinCostJoinOutput>& checkMe) { 
           Handle<Q02MinCostSelectionOutput> ret = makeObject<Q02MinCostSelectionOutput> (*checkMe);
           return ret; 
        });
    }


};

/*
    val europe = region.filter($"r_name" === "EUROPE")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
    //.select($"r_regionkey", $"n_regionkey", $"s_suppkey", $"n_nationkey", $"s_nationkey", $"p_partkey", $"p_mfgr", $"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"s_address", $"s_phone", $"s_comment")

    val brass = part.filter(part("p_size") === 15 && part("p_type").endsWith("BRASS"))
      .join(europe, europe("ps_partkey") === $"p_partkey")
    //.cache

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))

    brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      .limit(100)
*/

}

#endif
