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
#ifndef QUERY13_H
#define QUERY13_H


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

class Q13TPCHOrderSelection : public SelectionComp<TPCHOrder, TPCHOrder> {

private:

   String word1;
   String word2;

public:

    ENABLE_DEEP_COPY

    Q13TPCHOrderSelection () {}

    Q13TPCHOrderSelection (std::string word1, std::string word2) {
        this->word1 = word1;
        this->word2 = word2;
    }

    Lambda<bool> getSelection(Handle<TPCHOrder> checkMe) override {
        return makeLambda(checkMe,
           [&](Handle<TPCHOrder>& checkMe) {
                std::string o_comment = checkMe->o_comment;
                std::string::size_type n = o_comment.find(this->word1, 0);
                if (n == std::string::npos) {
                    return false;
                }
                n = o_comment.find(this->word2, n+sizeof(this->word1));
                if (n == std::string::npos) {
                    return false;
                }
                return true;

           });
    }

    Lambda<Handle<TPCHOrder>> getProjection(Handle<TPCHOrder> checkMe) override {
        return makeLambda(checkMe, [](Handle<TPCHOrder>& checkMe) { return checkMe; });
    }


};

class Q13TPCHCustomerTPCHOrders : public Object {

public:

    ENABLE_DEEP_COPY

    Q13TPCHCustomerTPCHOrders () {}

    Q13TPCHCustomerTPCHOrders (int o_orderkey, int c_custkey) {
        this->o_orderkey = o_orderkey;
        this->c_custkey = c_custkey;
    }

    int o_orderkey;

    int c_custkey;

};

class Q13TPCHCustomerTPCHOrderJoin : public JoinComp<Q13TPCHCustomerTPCHOrders, TPCHCustomer, TPCHOrder> {

public:

    ENABLE_DEEP_COPY

    Q13TPCHCustomerTPCHOrderJoin () {}

    Lambda<bool> getSelection (Handle<TPCHCustomer> in1, Handle<TPCHOrder> in2) override {
        return makeLambdaFromMember(in1, c_custkey) == makeLambdaFromMember(in2,
           o_custkey);
         
    }
    
    Lambda<Handle<Q13TPCHCustomerTPCHOrders>> getProjection(Handle<TPCHCustomer> in1, Handle<TPCHOrder> in2) override {
        return makeLambda(in1, in2, [](Handle<TPCHCustomer>& in1, Handle<TPCHOrder>& in2) {
            Handle<Q13TPCHCustomerTPCHOrders> ret = makeObject<Q13TPCHCustomerTPCHOrders>(in2->o_orderkey, 
               in1->c_custkey);
            return ret;
        });
    }


};


class Q13CountResult : public Object {

public:

    int key;
    int value;

    ENABLE_DEEP_COPY

    Q13CountResult () {}

    int & getKey() {
        return key;
    }

    int & getValue() {
        return value;
    }

};


class Q13TPCHOrdersPerTPCHCustomer : public AggregateComp<Q13CountResult,
                                               Q13TPCHCustomerTPCHOrders,
                                               int,
                                               int> {

public:

    ENABLE_DEEP_COPY


    Q13TPCHOrdersPerTPCHCustomer () {}

    Lambda<int> getKeyProjection(Handle<Q13TPCHCustomerTPCHOrders> aggMe) override {
         return makeLambdaFromMember(aggMe, c_custkey);
    }

    Lambda<int> getValueProjection (Handle<Q13TPCHCustomerTPCHOrders> aggMe) override {
         return makeLambda(aggMe, [](Handle<Q13TPCHCustomerTPCHOrders>& aggMe) {
             return 1;
         });
    }


};


class Q13TPCHCustomerDistribution : public AggregateComp<Q13CountResult,
                                               Q13CountResult,
                                               int,
                                               int> {

public:

    ENABLE_DEEP_COPY


    Q13TPCHCustomerDistribution () {}

    Lambda<int> getKeyProjection(Handle<Q13CountResult> aggMe) override {
         return makeLambdaFromMember(aggMe, value);
    }

    Lambda<int> getValueProjection (Handle<Q13CountResult> aggMe) override {
         return makeLambda(aggMe, [](Handle<Q13CountResult>& aggMe) {
             return 1;
         });
    }


};

/*
    customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)
*/

}

#endif
