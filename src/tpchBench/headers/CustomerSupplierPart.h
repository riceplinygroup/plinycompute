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

#include "SupplierPart.h"


// This class represents a triple that holds a triple of (customerName, SupplierName, PartID)


class CustomerSupplierPart: public pdb::Object {

public:
	pdb::String customerName;
	pdb::Vector<SupplierPart> supplierPart;

	ENABLE_DEEP_COPY

	//Default constructor:
	CustomerSupplierPart() {}

	//Default destructor:
	~CustomerSupplierPart() {}

	//Constructor with arguments:
	CustomerSupplierPart(pdb::String customerName) {
		this->customerName = customerName;
	}

	void print() {
		std::cout<<"Customer: " << customerName.c_str() << " [ ";
		for (int i = 0; i < supplierPart.size(); ++i) {
			pdb::String supplierName = supplierPart[i].getSupplierName();
			int partIDs= supplierPart[i].getPartKey();
			std::cout<<"(" << supplierName.c_str() << "," << partIDs << ")";
		}
			 std::cout<<"  ] "<<std::endl;
	}

	const pdb::String& getCustomerName() const {
		return customerName;
	}

	void setCustomerName(const pdb::String & customerName) {
		this->customerName = customerName;
	}

	const pdb::Vector<SupplierPart>& getSupplierPart() const {
		return supplierPart;
	}

	void setSupplierPart(const pdb::Vector<SupplierPart>& supplierPart) {
		this->supplierPart = supplierPart;
	}
};

#endif

