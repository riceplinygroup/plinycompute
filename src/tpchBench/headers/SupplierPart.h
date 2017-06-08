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

// This class represents the aggregate final results a tuple of (customerName, map(supplierName, Vector(PartID)))
namespace pdb {

class SupplierPart: public pdb::Object {

public:
	pdb::Handle<pdb::String> customerName;

	// for each customer the supplier sold to, the list of all partIDs sold
	pdb::Handle<pdb::Map<pdb::String, pdb::Vector<int>>>soldPartIDs;

	ENABLE_DEEP_COPY

	// Default constructor:
	SupplierData() {}

	// Default destructor:
	~SupplierData() {}

	// Constructor with arguments
	SupplierData(pdb::Handle<pdb::String> customerName, pdb::Handle<pdb::Map<pdb::String, pdb::Vector<int>>> soldPartIDs) {
		this->customerName= customerName;
		this->soldPartIDs= soldPartIDs;
	}

};


}
#endif
