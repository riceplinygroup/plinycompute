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
#ifndef SUPPLIER_DATA_H
#define SUPPLIER_Data_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"

// This class represents the aggregate final results a tuple of (customerName, map(supplierName, Vector(PartID)))
namespace pdb {

class SupplierData: public pdb::Object {

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

// We need to overload the plus operator to merge two maps of Map<String, Vector<int>>
inline Handle<Map<String, Vector<int>>>   &operator+ (Handle<Map<String, Vector<int>>> &lhs, Handle<Map<String, Vector<int>>> &rhs) {
	auto iter = rhs->begin();
	while (iter != rhs->end()) {
		String myKey = (*iter).key;
		if (lhs->count(myKey) == 0) {
			try {
				(*lhs)[myKey] = (*iter).value;
			} catch ( NotEnoughSpace &n ) {
				lhs->setUnused (myKey);
				throw n;
			}
		} else {

			size_t mySize = (*lhs)[myKey].size();
			size_t otherSize = (*iter).value.size();
			for (size_t i = mySize; i < mySize + otherSize; i++) {
				try {

					(*lhs)[myKey].push_back((*iter).value[i-mySize]);

				} catch (NotEnoughSpace &n) {

					size_t curSize = (*lhs)[myKey].size();
					for (size_t j = mySize; j < curSize; j++) {
						(*lhs)[myKey].pop_back();
					}
					for (size_t j = 0; j < (*lhs)[myKey].size(); j++) {
						std :: cout << j << ": " << (*lhs)[myKey][j]<< ";";
					}
					throw n;

				}

			}
		}
		++iter;
	}
	return lhs;

}
}
#endif
