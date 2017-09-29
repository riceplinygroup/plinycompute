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

#include "SupplierInfo.h"

using namespace pdb;
class CustomerMultiSelection: public MultiSelectionComp<SupplierInfo, Customer> {

public:
	ENABLE_DEEP_COPY

	CustomerMultiSelection() {
	}

	// Select all of the Customer Objects
	Lambda<bool> getSelection(Handle<Customer> checkMe) override {
		return makeLambda(checkMe, [] (Handle<Customer> & checkMe) {return true;});
	}

	// gets the CustomerName, Supplier and PartKeu out of the Customer objects and makes a vector of  customerSupplierPartFlat objects
	Lambda<Vector<Handle<SupplierInfo>>> getProjection (Handle <Customer> checkMe) override {

		return makeLambda (checkMe, [] (Handle<Customer>& checkMe) {
                                        Customer & myCustomer = *checkMe;
                                        String myCustomerName = myCustomer.getName();
					pdb::Vector<Order> &  m_orders= myCustomer.getOrders();
                                        size_t mySize = m_orders.size();
                                        Order * myOrders = m_orders.c_ptr();                                           
					pdb::Vector<pdb::Handle<SupplierInfo>> customerSupplierPartFlat_vector (mySize);
					// get the orders
					for (int i = 0; i < mySize; i++) {
						pdb::Vector<LineItem> & lineItems = myOrders[i].getLineItems();
                                                size_t myLineItemSize = lineItems.size();
                                                LineItem * myLineItems = lineItems.c_ptr();
						// get the LineItems
						for (int j = 0; j < myLineItemSize; j++) {
                                                        LineItem & myLineItem = myLineItems[j];
							Handle<Supplier>  supplier = myLineItem.getSupplier();
							Handle<Part> part = myLineItem.getPart();

							pdb::Handle<SupplierInfo>  supplierPart = pdb::makeObject<SupplierInfo> (supplier->getName(), myCustomerName, part->getPartKey());
							customerSupplierPartFlat_vector.push_back(supplierPart);
						}
					}


					return customerSupplierPartFlat_vector;
				});
	}
};
#endif
