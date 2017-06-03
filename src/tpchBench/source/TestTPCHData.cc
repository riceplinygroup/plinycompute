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
#ifndef TEST_DataTPCH_CC
#define TEST_DataTPCH_CC

#include "CatalogClient.h"

// this won't be visible to the v-table map, since it is not in the built in types directory
#include "Part.h"
#include "Supplier.h"
#include "LineItem.h"
#include "Order.h"
#include "Customer.h"

int main() {

	// register the shared employee class
	pdb::CatalogClient pdbClient(8108, "localhost", make_shared<pdb::PDBLogger>("clientLog"));

	string errMsg;
	if (!pdbClient.registerType("libraries/libPart.so", errMsg))
		cout << "Not able to register type.\n";

	if (!pdbClient.registerType("libraries/libSupplier.so", errMsg))
		cout << "Not able to register type.\n";

	if (!pdbClient.registerType("libraries/libLineItem.so", errMsg))
		cout << "Not able to register type.\n";

	if (!pdbClient.registerType("libraries/libOrder.so", errMsg))
		cout << "Not able to register type.\n";

	if (!pdbClient.registerType("libraries/libCustomer.so", errMsg))
		cout << "Not able to register type.\n";

	pdb::makeObjectAllocatorBlock(8192 * 1024, true);

	// since SharedEmployee is not a builtin object, this will cause a request to the catalog to
	// obtain the type code for the SharedEmployee class

	pdb::Handle<pdb::Vector<pdb::Handle<Customer>>>customers = pdb::makeObject<pdb::Vector<pdb::Handle<Customer>>> ();

	int maxPartsInEachLineItem = 10;
	int maxLineItemsInEachOrder = 20;
	int maxOrderssInEachCostomer = 20;

	// Make Customers
	for (int customerID = 0; customerID < 10; ++customerID) {

		pdb::Handle<pdb::Vector<pdb::Handle<Order>>>orders = pdb::makeObject<pdb::Vector<pdb::Handle<Order>>> ();

			// Make LineItems
			for (int lineItemID = 0; lineItemID < maxLineItemsInEachOrder; ++lineItemID) {

				pdb::Handle<pdb::Vector<pdb::Handle<LineItem>>>lineItems = pdb::makeObject<pdb::Vector<pdb::Handle<LineItem>>> ();

				for (int partID = 0; partID < maxPartsInEachLineItem; ++partID) {
					//1.  Make Part and Supplier
					pdb::Handle<Part> part = pdb::makeObject<Part>(partID, "Part1", "mfgr", "Brand1", "type1", partID, "Container1", 12.1, "Comment1");
					pdb::Handle<Supplier> supplier = pdb::makeObject<Supplier>(partID, "Part1", "address", partID, "Phone1", 12.1, "Comment1");

					//2. Make LineItem
					pdb::Handle<LineItem> lineItem = pdb::makeObject<LineItem>("Linetem1", partID, supplier, part, partID, 12.1, 12.1, 12.1, 12.1, "ReturnFlag1", "lineStatus1", "shipDate", "commitDate", "receiptDate",
							"sgipingStruct", "shipMode1", "Comment1");

					//3. Add the LineItem to the LineItem Vector
					lineItems->push_back(lineItem);
				}

				//4. Make Order
				pdb::Handle<Order> order = pdb::makeObject<Order>(lineItems, lineItemID,  1, "orderStatus", 1, "orderDate", "OrderPriority", "clerk", 1, "Comment1");
				orders->push_back(order);
			}

			pdb::Handle<Customer> customer = pdb::makeObject<Customer>(orders, customerID,  "customerName", "address",1, "phone", 12.1,"mktsegment", "Comment1");
			customers->push_back(customer);
	}






}

#endif

