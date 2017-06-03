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


class Customer: public pdb::Object {

private:
	pdb::Handle<pdb::Vector<pdb::Handle<Order>>>orders;
	int custKey;
	pdb::Handle<pdb::String> name;
	pdb::Handle<pdb::String> address;
	int nationKey;
	pdb::Handle<pdb::String> phone;
	double accbal;
	pdb::Handle<pdb::String> mktsegment;
	pdb::Handle<pdb::String> comment;

public:

	ENABLE_DEEP_COPY

	~Customer() {
	}

	Customer() {}

	// Constructor with arguments using std::string

	Customer(pdb::Handle<pdb::Vector<pdb::Handle<Order>>>orders, int custKey,
			std:: string name, std:: string address,
			int nationKey, std:: string phone, double accbal,
			std:: string mktsegment, std:: string comment) {

		this->orders=orders;
		this->custKey=custKey;
		this->name= pdb::makeObject <pdb::String> (name);
		this->address= pdb::makeObject <pdb::String>(address);
		this->nationKey=nationKey;
		this->phone=pdb::makeObject <pdb::String> (phone);
		this->accbal=accbal;
		this->mktsegment=pdb::makeObject <pdb::String> (mktsegment);
		this->comment=pdb::makeObject <pdb::String> (comment);
	}

	double getAccbal() const
	{
		return accbal;
	}

	void setAccbal(double accbal)
	{
		this->accbal = accbal;
	}

	const pdb::Handle<pdb::String>& getAddress() const
	{
		return address;
	}

	void setAddress(const pdb::Handle<pdb::String>& address)
	{
		this->address = address;
	}

	const pdb::Handle<pdb::String>& getComment() const
	{
		return comment;
	}

	void setComment(const pdb::Handle<pdb::String>& comment)
	{
		this->comment = comment;
	}

	int getCustKey() const
	{
		return custKey;
	}

	void setCustKey(int custKey)
	{
		this->custKey = custKey;
	}

	const pdb::Handle<pdb::String>& getMktsegment() const
	{
		return mktsegment;
	}

	void setMktsegment(const pdb::Handle<pdb::String>& mktsegment)
	{
		this->mktsegment = mktsegment;
	}

	const pdb::Handle<pdb::String>& getName() const
	{
		return name;
	}

	void setName(const pdb::Handle<pdb::String>& name)
	{
		this->name = name;
	}

	int getNationKey() const
	{
		return nationKey;
	}

	void setNationKey(int nationKey)
	{
		this->nationKey = nationKey;
	}

	const pdb::Handle<pdb::Vector<pdb::Handle<Order> > >& getOrders() const
	{
		return orders;
	}

	void setOrders(const pdb::Handle<pdb::Vector<pdb::Handle<Order> > >& orders)
	{
		this->orders = orders;
	}

	const pdb::Handle<pdb::String>& getPhone() const
	{
		return phone;
	}

	void setPhone(const pdb::Handle<pdb::String>& phone)
	{
		this->phone = phone;
	}
};

#endif
