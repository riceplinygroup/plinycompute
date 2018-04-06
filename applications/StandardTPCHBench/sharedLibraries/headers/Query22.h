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
#ifndef QUERY22_H
#define QUERY22_H


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
#include "SumResult.h"
#include <cstring>

using namespace pdb;

namespace tpch {


class Q22TPCHCustomerAccbalAvg : public AggregateComp<AvgResult,
                                               TPCHCustomer,
                                               int,
                                               Avg> {

public:

    ENABLE_DEEP_COPY

    String i1;
    String i2;
    String i3;
    String i4;
    String i5;
    String i6;
    String i7;


    Q22TPCHCustomerAccbalAvg () {}

    Q22TPCHCustomerAccbalAvg (std::string i1, std::string i2, std::string i3,
              std::string i4, std::string i5, std::string i6, std::string i7) {
        this->i1 = i1;
        this->i2 = i2;
        this->i3 = i3;
        this->i4 = i4;
        this->i5 = i5;
        this->i6 = i6;
        this->i7 = i7;
    }

    Lambda<int> getKeyProjection(Handle<TPCHCustomer> aggMe) override {
         return makeLambda(aggMe, [](Handle<TPCHCustomer>& aggMe) {
             return 0;
         });;
    }

    Lambda<Avg> getValueProjection (Handle<TPCHCustomer> aggMe) override {
         return makeLambda(aggMe, [&](Handle<TPCHCustomer>& aggMe) {
             Avg ret;
             TPCHCustomer me = *aggMe;
             std::string c_phone = me.c_phone;
             if ((me.c_acctbal > 0.00) && ((strcmp(c_phone.substr(0, 2).c_str(), i1.c_str())==0) 
                 || (strcmp(c_phone.substr(0, 2).c_str(), i2.c_str())==0) 
                 || (strcmp(c_phone.substr(0, 2).c_str(), i3.c_str())==0)
                 || (strcmp(c_phone.substr(0, 2).c_str(), i4.c_str())==0) 
                 || (strcmp(c_phone.substr(0, 2).c_str(), i5.c_str())==0)
                 || (strcmp(c_phone.substr(0, 2).c_str(), i6.c_str())==0) 
                 || (strcmp(c_phone.substr(0, 2).c_str(), i7.c_str())==0))) {
                 ret.total = me.c_acctbal;
                 ret.count = 1;
             } else {
                 ret.total = 0;
                 ret.count = 0;
             }               
             return ret;
         });
    }


};

class Q22TPCHOrderCountPerTPCHCustomer : public AggregateComp<SumResult,
                                               TPCHOrder,
                                               int,
                                               int> {

public:

    ENABLE_DEEP_COPY

    Q22TPCHOrderCountPerTPCHCustomer () {}

    Lambda<int> getKeyProjection(Handle<TPCHOrder> aggMe) override {
         return makeLambdaFromMember(aggMe, o_custkey);
    }

    Lambda<int> getValueProjection (Handle<TPCHOrder> aggMe) override {
         return makeLambda(aggMe, [](Handle<TPCHOrder>& aggMe) {
             return 1;
         });
    }


};


class Q22TPCHOrderCountSelection : public SelectionComp<SumResult, SumResult> {

public:

    ENABLE_DEEP_COPY

    Q22TPCHOrderCountSelection () {}

    Lambda<bool> getSelection (Handle<SumResult> checkMe) override {
        return makeLambda(checkMe, [](Handle<SumResult> & checkMe) {
             if (checkMe->total == 1) {
                 return true;
             } else {
                 return false;
             }
         });
    }

    Lambda<Handle<SumResult>> getProjection (Handle<SumResult> checkMe) override {
        return makeLambda(checkMe,  [](Handle<SumResult> & checkMe) {
             return checkMe;
        });
    }

};


class Q22JoinedCntryBal : public Object {

public:

    ENABLE_DEEP_COPY

    Q22JoinedCntryBal () {}

    Q22JoinedCntryBal (std::string& cntrycode, double& c_acctbal) {

        this->cntrycode = cntrycode;
        this->c_acctbal = c_acctbal;

    }
    

    String cntrycode;

    double c_acctbal;


};



class Q22CntryBalJoin : public JoinComp<Q22JoinedCntryBal, SumResult, TPCHCustomer, AvgResult> {

public:

    ENABLE_DEEP_COPY

    Q22CntryBalJoin () {}

    Lambda<bool> getSelection (Handle<SumResult> in1, Handle<TPCHCustomer> in2, Handle<AvgResult> in3) override {
        return (makeLambdaFromMember(in1, identifier) == makeLambdaFromMember(in2,
           c_custkey)) && makeLambda(in2, in3, [](Handle<TPCHCustomer>& in2, Handle<AvgResult>& in3) {
             if (in2->c_acctbal > in3->getAvg()) {
                 return true;
             } else {
                 return false;
             }
         });

    }

    Lambda<Handle<Q22JoinedCntryBal>> getProjection(Handle<SumResult> in1, Handle<TPCHCustomer> in2, Handle<AvgResult> in3) override {
        return makeLambda(in1, in2, in3, [](Handle<SumResult>& in1, Handle<TPCHCustomer>& in2, Handle<AvgResult> in3) {
            TPCHCustomer me = *in2;
            std::string c_phone = me.c_phone;
            Handle<Q22JoinedCntryBal> ret = makeObject<Q22JoinedCntryBal>(c_phone.substr(0, 2), me.c_acctbal);
            return ret;
        });
    }

};


class Q22AggregatedCntryBal : public Object {

public:

    ENABLE_DEEP_COPY

    Q22AggregatedCntryBal () {}

    String cntrycode;

    Avg avg;

    String & getKey() {
        return cntrycode;
    }

    Avg & getValue() {
        return avg;
    }

};


class Q22CntryBalAgg : public AggregateComp<Q22AggregatedCntryBal,
                                               Q22JoinedCntryBal,
                                               String,
                                               Avg> {

public:

    ENABLE_DEEP_COPY


    Q22CntryBalAgg () {}

    Lambda<String> getKeyProjection(Handle<Q22JoinedCntryBal> aggMe) override {
         return makeLambdaFromMember(aggMe, cntrycode);
    }

    Lambda<Avg> getValueProjection (Handle<Q22JoinedCntryBal> aggMe) override {
         return makeLambda(aggMe, [](Handle<Q22JoinedCntryBal>& aggMe) {
             Avg ret(aggMe->c_acctbal, 1);
             return ret;
         });
    }


};


}

#endif
