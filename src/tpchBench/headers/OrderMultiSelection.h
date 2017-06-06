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
#ifndef ORDER_MULTI_SELECT_H
#define ORDER_MULTI_SELECT_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"

#include "PDBVector.h"
#include "PDBString.h"

#include "Customer.h"
#include "Order.h"
#include "LineItem.h"

using namespace pdb;
class OrderMultiSelection: public MultiSelectionComp<LineItem, Order> {

public:

	ENABLE_DEEP_COPY

	OrderMultiSelection() {
	}

	// Select all of the Order Objects
	Lambda<bool> getSelection(Handle<Order> checkMe) override {
		return makeLambda(checkMe, [] (Handle<Order> & checkMe) {return true;});
	}

	// then get only the LineItem out of the Order objects
	Lambda<Vector<Handle<LineItem>>> getProjection (Handle <Order> checkMe) override {
		return makeLambdaFromMember (checkMe, lineItems);
//		return makeLambdaFromMethod (checkMe, getLineItems);

	}
};

#endif
