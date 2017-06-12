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
#ifndef SUPPLIER_PART_H
#define SUPPLIER_PART_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"

// This class represents a triple that holds a tuple of (customerName, (SupplierName, Vector<PartID>))


class SupplierPart: public pdb::Object {

public:
	pdb::String supplierName;
	int  partKey;

	ENABLE_DEEP_COPY

	//Default constructor:
	SupplierPart() {}

	//Default destructor:
	~SupplierPart() {}

	//Constructor with arguments:
	SupplierPart(pdb::String supplierName, int  partKey) {
		this->supplierName = supplierName;
		this->partKey = partKey;
	}

	int getPartKey() const {
		return partKey;
	}

	void setPartKey(int partKey) {
		this->partKey = partKey;
	}

	const pdb::String getSupplierName() const {
		return supplierName;
	}

	void setSupplierName(pdb::String supplierName) {
		this->supplierName = supplierName;
	}
};

#endif

