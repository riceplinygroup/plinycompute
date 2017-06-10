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
#ifndef CUSTOMER_SUPPLIER_PART_H
#define CUSTOMER_SUPPLIER_PART_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"

// This class represents a triple that holds a triple of (customerName, SupplierName, PartID)
namespace pdb {

class CustomerSupplierPart: public pdb::Object {

public:
	Handle<String> customerName;
	Handle<Map<String, Vector<int>>> soldPartIDs;

	ENABLE_DEEP_COPY

	//Default constructor:
	CustomerSupplierPart() {}

	//Default destructor:
	~CustomerSupplierPart() {}

	//Constructor with arguments:
	CustomerSupplierPart(pdb::Handle<pdb::String> customerName) {
		this->customerName = customerName;
		this->soldPartIDs = pdb::makeObject<pdb::Map<pdb::String, pdb::Vector<int>>>();
	}

	const pdb::Handle<pdb::String>& getCustomerName() const {
		return customerName;
	}

	void setCustomerName(const pdb::Handle<pdb::String>& customerName) {
		this->customerName = customerName;
	}

	void addSupplierPart(pdb::String supplierName, int partKey) {

		if(soldPartIDs->count(supplierName)==0) {
			// not found
			pdb::Handle<pdb::Vector<int>> partKeyVector = pdb::makeObject<pdb::Vector<int>>();
			partKeyVector->push_back(partKey);
			(*soldPartIDs)[supplierName] = * partKeyVector;

		} else {
			// found
			pdb::Vector<int> existing_partKeyVector = (*soldPartIDs)[supplierName];
			existing_partKeyVector.push_back(partKey);
			(*soldPartIDs)[supplierName] = existing_partKeyVector;
		}
	}

	const Handle<Map<String,Vector<int> > >& getSoldPartIDs() const
	{
		return soldPartIDs;
	}

	void setSoldPartIDs(const Handle<Map<String,Vector<int> > >& soldPartIDs)
	{
		this->soldPartIDs = soldPartIDs;
	}

	void print() {

		auto iter = soldPartIDs->begin();

		while (iter != soldPartIDs->end()) {

			std::cout<<"Customer: " << customerName->c_str() << "[ ";

			pdb::String myKey = (*iter).key;
			pdb::Vector<int> partIDs= (*soldPartIDs)[myKey];

			std::cout<<"Supplier Key: " << myKey.c_str() << "( ";

			for (int i = 0; i < partIDs.size(); ++i) {
				std::cout<<" " <<partIDs[i] << ",";
			}

			std::cout<<") ] "<<std::endl;
			 ++iter;


		}

	}

};
}
#endif

