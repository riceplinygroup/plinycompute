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

#ifndef CUSTOMER_MAP_SELECTION_H
#define CUSTOMER_MAP_SELECTION_H

#include "Lambda.h"
#include "SelectionComp.h"

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"

#include "PDBVector.h"
#include "PDBString.h"

#include "Customer.h"
#include "CustomerSupplierPart.h"



using namespace pdb;
class CustomerMapSelection : public SelectionComp <CustomerSupplierPart, Customer> {


	// This computation maps a Customer Object to a CustomerSupplierPart object

public:

	ENABLE_DEEP_COPY

	CustomerMapSelection () {}

	// Select all of the Customer Objects
	Lambda<bool> getSelection(Handle<Customer> checkMe) override {
		return makeLambda(checkMe, [] (Handle<Customer> & checkMe) {return true;});
	}


	// Then get the Orders out of the Customers
	Lambda<Handle<CustomerSupplierPart>> getProjection (Handle <Customer> checkMe) override {
		return makeLambda (checkMe, [] (Handle<Customer>& checkMe) {

			pdb::Handle<CustomerSupplierPart> customerSupplierPart = pdb::makeObject<CustomerSupplierPart>(checkMe->getName());

					pdb::Vector<pdb::Handle<Order>> m_orders= *checkMe-> orders;

					// get the orders
					for (int i = 0; i < m_orders.size(); i++) {
						auto lineItems = m_orders[i]->getLineItems();

						// get the LineItems
						for (int j = 0; j < lineItems->size(); j++) {
							auto supplier = (*lineItems)[j]->getSupplier();

							String  supplierName= *(supplier->getName());

							auto part = (*lineItems)[j]->getPart();
							int partKey= part->getPartKey();

							customerSupplierPart->addSupplierPart(supplierName, partKey);

						}
					}

					return customerSupplierPart;
				});
	}
};
#endif
