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
#ifndef ORDER_H
#define ORDER_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"
#include "LineItem.h"

// This class represents a Oder Object according to the TPC-H Database Benchmark


// CREATE TABLE ORDERS ( O_ORDERKEY INTEGER NOT NULL,
// O_CUSTKEY INTEGER NOT NULL,
// O_ORDERSTATUS CHAR(1) NOT NULL,
// O_TOTALPRICE DECIMAL(15,2) NOT NULL,
// O_ORDERDATE DATE NOT NULL,
// O_ORDERPRIORITY CHAR(15) NOT NULL,
// O_CLERK CHAR(15) NOT NULL,
// O_SHIPPRIORITY INTEGER NOT NULL,
// O_COMMENT VARCHAR(79) NOT NULL);

class Order : public pdb::Object {

public:
    pdb::Vector<LineItem> lineItems;

    int orderKey;
    int custKey;
    pdb::String orderStatus;
    double totalPrice;
    pdb::String orderDate;
    pdb::String orderPriority;
    pdb::String clerk;
    int shipPriority;
    pdb::String comment;


    ENABLE_DEEP_COPY

    ~Order() {}

    Order() {}

    Order(pdb::Vector<LineItem> lineItems,
          int orderkey,
          int custkey,
          std::string orderstatus,
          double totalprice,
          std::string orderdate,
          std::string orderpriority,
          std::string clerk,
          int shippriority,
          std::string comment) {
        this->lineItems = lineItems;
        this->orderKey = orderkey;
        this->custKey = custkey;
        this->orderStatus = orderstatus;
        this->totalPrice = totalprice;
        this->orderDate = orderdate;
        this->orderPriority = orderpriority;
        this->clerk = clerk;
        this->shipPriority = shippriority;
        this->comment = comment;
    }

    pdb::String getClerk() {
        return clerk;
    }

    void setClerk(pdb::String clerk) {
        this->clerk = clerk;
    }

    pdb::String getComment() {
        return comment;
    }

    void setComment(pdb::String comment) {
        this->comment = comment;
    }

    int getCustKey() {
        return custKey;
    }

    void setCustKey(int custKey) {
        this->custKey = custKey;
    }

    pdb::Vector<LineItem>& getLineItems() {
        return lineItems;
    }

    void setLineItems(pdb::Vector<LineItem>& lineItems) {
        this->lineItems = lineItems;
    }

    pdb::String getOrderDate() {
        return orderDate;
    }

    void setOrderDate(pdb::String orderDate) {
        this->orderDate = orderDate;
    }

    int getOrderKey() {
        return orderKey;
    }

    void setOrderKey(int orderKey) {
        this->orderKey = orderKey;
    }

    pdb::String getOrderPriority() {
        return orderPriority;
    }

    void setOrderPriority(pdb::String orderPriority) {
        this->orderPriority = orderPriority;
    }

    pdb::String getOrderStatus() {
        return orderStatus;
    }

    void setOrderStatus(pdb::String orderStatus) {
        this->orderStatus = orderStatus;
    }

    int getShipPriority() {
        return shipPriority;
    }

    void setShipPriority(int shipPriority) {
        this->shipPriority = shipPriority;
    }

    double getTotalPrice() {
        return totalPrice;
    }

    void setTotalPrice(double totalPrice) {
        this->totalPrice = totalPrice;
    }
};
#endif
