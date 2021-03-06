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
#ifndef CUSTOMER_H
#define CUSTOMER_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"

#include "Order.h"

// This class represents a Customer Object according to the TPC-H Database Benchmark

//	CREATE TABLE CUSTOMER (
//		C_CUSTKEY		SERIAL PRIMARY KEY,
//		C_NAME			VARCHAR(25),
//		C_ADDRESS		VARCHAR(40),
//		C_NATIONKEY		BIGINT NOT NULL, -- references N_NATIONKEY
//		C_PHONE			CHAR(15),
//		C_ACCTBAL		DECIMAL,
//		C_MKTSEGMENT	CHAR(10),
//		C_COMMENT		VARCHAR(117)
//	);


class Customer : public pdb::Object {


public:
    pdb::Vector<Order> orders;
    int custKey;
    pdb::String name;
    pdb::String address;
    int nationKey;
    pdb::String phone;
    double accbal;
    pdb::String mktsegment;
    pdb::String comment;


    ENABLE_DEEP_COPY

    ~Customer() {}

    Customer() {}

    // Constructor with arguments using std::string

    Customer(pdb::Vector<Order> orders,
             int custKey,
             std::string name,
             std::string address,
             int nationKey,
             std::string phone,
             double accbal,
             std::string mktsegment,
             std::string comment) {
        this->orders = orders;
        this->custKey = custKey;
        this->name = name;
        this->address = address;
        this->nationKey = nationKey;
        this->phone = phone;
        this->accbal = accbal;
        this->mktsegment = mktsegment;
        this->comment = comment;
    }

    double getAccbal() const {
        return accbal;
    }

    void setAccbal(double accbal) {
        this->accbal = accbal;
    }

    const pdb::String& getAddress() const {
        return address;
    }

    void setAddress(const pdb::String& address) {
        this->address = address;
    }

    const pdb::String& getComment() const {
        return comment;
    }

    void setComment(const pdb::String& comment) {
        this->comment = comment;
    }

    int getCustKey() const {
        return custKey;
    }

    void setCustKey(int custKey) {
        this->custKey = custKey;
    }

    const pdb::String& getMktsegment() const {
        return mktsegment;
    }

    void setMktsegment(const pdb::String& mktsegment) {
        this->mktsegment = mktsegment;
    }

    pdb::String& getName() {
        return name;
    }

    void setName(pdb::String& name) {
        this->name = name;
    }

    int getNationKey() const {
        return nationKey;
    }

    void setNationKey(int nationKey) {
        this->nationKey = nationKey;
    }

    pdb::Vector<Order>& getOrders() {
        return orders;
    }

    void setOrders(pdb::Vector<Order>& orders) {
        this->orders = orders;
    }

    const pdb::String& getPhone() const {
        return phone;
    }

    void setPhone(const pdb::String& phone) {
        this->phone = phone;
    }
};

#endif
