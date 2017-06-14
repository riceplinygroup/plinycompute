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
#ifndef CustomerSupplierPartFLATGroupBy_H
#define CustomerSupplierPartFLATGroupBy_H

#include "ClusterAggregateComp.h"

#include "Lambda.h"
#include "LambdaCreationFunctions.h"

#include "PDBVector.h"
#include "PDBString.h"

#include "SupplierPart.h"
#include "CustomerSupplierPart.h"
#include "CustomerSupplierPartFlat.h"

using namespace pdb;

// template <class OutputClass, class InputClass, class KeyClass, class ValueClass>
class CustomerSupplierPartFlatGroupBy: public ClusterAggregateComp<CustomerSupplierPart, CustomerSupplierPartFlat, pdb::String, pdb::Vector<SupplierPart>> {

public:

	ENABLE_DEEP_COPY

	CustomerSupplierPartFlatGroupBy() {
	}

	// the below constructor is NOT REQUIRED
	// user can also set output later by invoking the setOutput (std :: string dbName, std :: string setName)  method
	CustomerSupplierPartFlatGroupBy(std::string dbName, std::string setName) {
		this->setOutput(dbName, setName);
	}

	// the key type must have == and size_t hash () defined
	Lambda<pdb::String> getKeyProjection(pdb::Handle<CustomerSupplierPartFlat> aggMe) override {
		return makeLambda(aggMe, [] (pdb::Handle <CustomerSupplierPartFlat> & aggMe) {
			pdb::String myKey = aggMe->getCustomerName();
			return myKey;
		});
	}

	// the value type must have + defined
	Lambda<pdb::Vector<SupplierPart>> getValueProjection(pdb::Handle<CustomerSupplierPartFlat> aggMe) override {
		return makeLambda(aggMe, [] (pdb::Handle <CustomerSupplierPartFlat> & aggMe) {

			pdb::Handle<pdb::Vector<SupplierPart>> ret = pdb::makeObject<pdb::Vector<SupplierPart>> ();
			pdb::Handle<SupplierPart> supplierPart = pdb::makeObject<SupplierPart> (aggMe->getSupplierName(), aggMe->getPartKey());

			ret->push_back(*supplierPart);
			return *ret;
		});
	}
};

namespace pdb {

inline pdb::Vector<SupplierPart> &operator+(pdb::Vector<SupplierPart> &lhs, pdb::Vector<SupplierPart> &rhs) {
	for (int i = 0; i < rhs.size(); ++i) {
		lhs.push_back(rhs[i]);
	}
	return lhs;
}
}
#endif
