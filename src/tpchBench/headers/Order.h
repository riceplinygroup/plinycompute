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

class Order: public pdb::Object {

public:

	pdb::Vector<pdb::Handle <LineItem>> lineItems;

	int orderKey;
	int custKey;
	pdb::Handle<pdb::String> orderStatus;
	double totalPrice;
	pdb::Handle<pdb::String> orderDate;
	pdb::Handle<pdb::String> orderPriority;
	pdb::Handle<pdb::String> clerk;
	int shipPriority;
	pdb::Handle<pdb::String> comment;




	ENABLE_DEEP_COPY

	~Order() {
	}

	Order() {
	}

	Order(pdb::Vector <pdb::Handle <LineItem>> lineItems, int orderkey,
			int custkey, std::string orderstatus,
			double totalprice, std::string orderdate,
			std::string orderpriority, std::string clerk,
			int shippriority, std::string comment){


		this->lineItems=lineItems;

//		for (auto lineItem: lineItems) {
//			this->lineItems.push_back(*lineItem);
//		}


		this->orderKey=orderkey;
		this->custKey=custkey;
		this->orderStatus=pdb::makeObject <pdb::String>(orderstatus);
		this->totalPrice=totalprice;
		this->orderDate=pdb::makeObject <pdb::String> (orderdate);
		this->orderPriority=pdb::makeObject <pdb::String>(orderpriority);
		this->clerk=pdb::makeObject <pdb::String>(clerk);
		this->shipPriority=shippriority;
		this->comment=pdb::makeObject <pdb::String>(comment);
	}

	pdb::Vector<pdb::Handle <LineItem>> getLineItems() const
	{
		return lineItems;
	}

	void setLineItems(pdb::Vector<pdb::Handle <LineItem>> lineItems)
	{
		this->lineItems = lineItems;
	}

	const pdb::Handle<pdb::String>& getClerk() const {
		return clerk;
	}

	void setClerk(const pdb::Handle<pdb::String>& clerk) {
		this->clerk = clerk;
	}

	const pdb::Handle<pdb::String>& getComment() const {
		return comment;
	}

	void setComment(const pdb::Handle<pdb::String>& comment) {
		this->comment = comment;
	}

	int getCustKey() const {
		return custKey;
	}

	void setCustKey(int custKey) {
		this->custKey = custKey;
	}

	const pdb::Handle<pdb::String>& getOrderDate() const {
		return orderDate;
	}

	void setOrderDate(const pdb::Handle<pdb::String>& orderDate) {
		this->orderDate = orderDate;
	}

	int getOrderKey() const {
		return orderKey;
	}

	void setOrderKey(int orderKey) {
		this->orderKey = orderKey;
	}

	const pdb::Handle<pdb::String>& getOrderPriority() const {
		return orderPriority;
	}

	void setOrderPriority(const pdb::Handle<pdb::String>& orderPriority) {
		this->orderPriority = orderPriority;
	}

	const pdb::Handle<pdb::String>& getOrderStatus() const {
		return orderStatus;
	}

	void setOrderStatus(const pdb::Handle<pdb::String>& orderStatus) {
		this->orderStatus = orderStatus;
	}

	int getShipPriority() const {
		return shipPriority;
	}

	void setShipPriority(int shipPriority) {
		this->shipPriority = shipPriority;
	}

	double getTotalPrice() const {
		return totalPrice;
	}

	void setTotalPrice(double totalPrice) {
		this->totalPrice = totalPrice;
	}
};
#endif
