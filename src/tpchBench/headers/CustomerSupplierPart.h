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

class CustomerSupplierPart:public pdb::Object{

public:
	pdb::Handle<pdb::String> customerName;
	pdb::Handle<pdb::String> supplierName;
	int partKey;


	ENABLE_DEEP_COPY

	//Default constructor:
	CustomerSupplierPart(){}

	//Default destructor:
	~CustomerSupplierPart() {}

	//Constructor with arguments:
	CustomerSupplierPart(std::string customerName, std::string  supplierName, int partKey) {
		this->partKey= partKey;
		this->customerName= pdb::makeObject<pdb::String>(customerName);
		this->supplierName=  pdb::makeObject<pdb::String>(supplierName);
	}

	const pdb::Handle<pdb::String>& getCustomerName() const {
		return customerName;
	}

	void setCustomerName(const pdb::Handle<pdb::String>& customerName) {
		this->customerName = customerName;
	}

	int getPartKey() const {
		return partKey;
	}

	void setPartKey(int partKey) {
		this->partKey = partKey;
	}

	const pdb::Handle<pdb::String>& getSupplierName() const {
		return supplierName;
	}

	void setSupplierName(const pdb::Handle<pdb::String>& supplierName) {
		this->supplierName = supplierName;
	}
};
#endif
