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
#ifndef CUSTOMER_SUPPLIER_PART_FLAT_H
#define CUSTOMER_SUPPLIER_PART_FLAT_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"

// This class represents a triple that holds a triple of (customerName, SupplierName, PartID)


class CustomerSupplierPartFlat: public pdb::Object {

public:
	pdb::String customerName;
	pdb::String supplierName;
	int  partKey;

	ENABLE_DEEP_COPY

	//Default constructor:
	CustomerSupplierPartFlat() {}

	//Default destructor:
	~CustomerSupplierPartFlat() {}

	//Constructor with arguments:
	CustomerSupplierPartFlat(pdb::String customerName, pdb::String supplierName, int  partKey) {
		this->customerName = customerName;
		this->supplierName=supplierName;
		this->partKey=partKey;
	}

	pdb::String getCustomerName()  {
		return customerName;
	}

	void setCustomerName( pdb::String customerName) {
		this->customerName = customerName;
	}

	int getPartKey()  {
		return partKey;
	}

	void setPartKey(int partKey) {
		this->partKey = partKey;
	}

	 pdb::String getSupplierName(){
		return supplierName;
	}

	void setSupplierName(pdb::String supplierName) {
		this->supplierName = supplierName;
	}
};

#endif

