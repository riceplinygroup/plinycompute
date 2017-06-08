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
#include "CustomerSupplierPart.h"

using namespace pdb;
class CustomerMultiSelection: public MultiSelectionComp<CustomerSupplierPart, Customer> {

public:

	ENABLE_DEEP_COPY

	CustomerMultiSelection() {
	}

	// Select all of the Customer Objects
	Lambda<bool> getSelection(Handle<Customer> checkMe) override {
		return makeLambda(checkMe, [] (Handle<Customer> & checkMe) {return true;});
	}

	// Then get the Orders out of the Customers

	Lambda<Vector<Handle<CustomerSupplierPart>>> getProjection (Handle <Customer> checkMe) override {

		return makeLambda (checkMe, [] (Handle<Customer>& checkMe) {

					pdb::Vector<pdb::Handle<Order>> m_orders= *checkMe-> orders;

					pdb::Handle<pdb::Vector<pdb::Handle<CustomerSupplierPart>>> customerSupplierPart_vector = pdb::makeObject<pdb::Vector<pdb::Handle<CustomerSupplierPart>>> ();

					// get the orders
					for (int i = 0; i < m_orders.size(); i++) {

						auto lineItems = m_orders[i]->getLineItems();

						// get the LineItems
						for (int j = 0; j < lineItems->size(); j++) {

							auto supplier = (*lineItems)[j]->getSupplier();
							auto part = (*lineItems)[j]->getPart();

							std::string customerName = checkMe->getName()->c_str();
							std::string supplierName = supplier->getName()->c_str();

							int partKey = part->getPartKey();

							std::cout<< "Customer Name: " << customerName<<std::endl;
							std::cout<< "Supplier Name: " << supplierName<<std::endl;
							std::cout<< "PartKey : " << partKey<<std::endl;

							// make a new customerSupplierPart object which is a triple representing the (customerName, supplierName, partKey)
							pdb::Handle<CustomerSupplierPart> customerSupplierPart = pdb::makeObject<CustomerSupplierPart>(customerName, supplierName, partKey);
							customerSupplierPart_vector->push_back(customerSupplierPart);
						}
					}
					return *customerSupplierPart_vector;
				});
	}
};
#endif
