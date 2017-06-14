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
#ifndef CUSTOMER_MULTI_SELECT_H
#define CUSTOMER_MULTI_SELECT_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"

#include "PDBVector.h"
#include "PDBString.h"

#include "Customer.h"
#include "Order.h"

#include "CustomerSupplierPartFlat.h"

using namespace pdb;
class CustomerMultiSelection: public MultiSelectionComp<CustomerSupplierPartFlat, Customer> {

public:
	ENABLE_DEEP_COPY

	CustomerMultiSelection() {
	}

	// Select all of the Customer Objects
	Lambda<bool> getSelection(Handle<Customer> checkMe) override {
		return makeLambda(checkMe, [] (Handle<Customer> & checkMe) {return true;});
	}

	// gets the CustomerName, Supplier and PartKeu out of the Customer objects and makes a vector of  customerSupplierPartFlat objects
	Lambda<Vector<Handle<CustomerSupplierPartFlat>>> getProjection (Handle <Customer> checkMe) override {

		return makeLambda (checkMe, [] (Handle<Customer>& checkMe) {

					pdb::Vector<Order> m_orders= checkMe-> getOrders();

					pdb::Handle<pdb::Vector<pdb::Handle<CustomerSupplierPartFlat>>> customerSupplierPartFlat_vector = pdb::makeObject<pdb::Vector<pdb::Handle<CustomerSupplierPartFlat>>> ();

					// get the orders
					for (int i = 0; i < m_orders.size(); i++) {
						auto lineItems = m_orders[i].getLineItems();

						// get the LineItems
						for (int j = 0; j < lineItems.size(); j++) {
							auto supplier = lineItems[j].getSupplier();
							auto part = lineItems[j].getPart();

							Handle<Vector<int>>  partKeyVector=pdb::makeObject<pdb::Vector<int>>();
							partKeyVector->push_back(part.getPartKey());

							pdb::Handle<CustomerSupplierPartFlat>  supplierPart = pdb::makeObject<CustomerSupplierPartFlat> (checkMe->getName(), supplier.getName(), *partKeyVector);
							customerSupplierPartFlat_vector->push_back(supplierPart);
						}
					}


					return *customerSupplierPartFlat_vector;
				});
	}
};
#endif
