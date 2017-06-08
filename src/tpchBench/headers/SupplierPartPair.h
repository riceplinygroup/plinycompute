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
#ifndef SUPPLIER_PART_PAIR_H
#define SUPPLIER_PART_PAIR_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"

// This class represents the aggregate final results a tuple of (customerName, map(supplierName, Vector(PartID)))
namespace pdb {

class SupplierPartPair: public pdb::Object {

public:
	int partKey;
	pdb::Handle<pdb::String> suppliertName;


	ENABLE_DEEP_COPY

	// Default constructor:
	SupplierPartPair() {}

	// Default destructor:
	~SupplierPartPair() {}

	// Constructor with arguments
	SupplierPartPair(pdb::Handle<pdb::String> supplierName, int partKey) {

	}

	int getPartKey() const {
		return partKey;
	}

	void setPartKey(int partKey) {
		this->partKey = partKey;
	}

	const pdb::Handle<pdb::String>& getSuppliertName() const {
		return suppliertName;
	}

	void setSuppliertName(const pdb::Handle<pdb::String>& suppliertName) {
		this->suppliertName = suppliertName;
	}
};


}
#endif
