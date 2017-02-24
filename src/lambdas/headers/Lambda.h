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

#ifndef LAMBDA_H
#define LAMBDA_H

#include <memory>
#include <vector>
#include <functional>
#include "Object.h"
#include "Handle.h"
#include "Ptr.h"
#include "TupleSpec.h"
#include "ComputeExecutor.h"
#include "LambdaHelperClasses.h"
#include "DereferenceLambda.h"

namespace pdb {

template <class ReturnType>
class Lambda {

private:

	// in case we wrap up a non-pointer type
	std :: shared_ptr <TypedLambdaObject <ReturnType>> tree;

	// does the actual tree traversal
	static void traverse (std :: map <std :: string, GenericLambdaObjectPtr> &fillMe, 
		GenericLambdaObjectPtr root, int &startLabel) {

		std :: string myName = root->getTypeOfLambda ();		
		myName = myName + "_" + std :: to_string (startLabel);
		std :: cout << "\tExtracted lambda named: " << myName << "\n";
		startLabel++;
		fillMe[myName] = root;
		for (int i = 0; i < root->getNumChildren (); i++) {
			GenericLambdaObjectPtr child = root->getChild (i);
			traverse (fillMe, child, startLabel);
		}	
	}

public:

	// create a lambda tree that returns a pointer
	Lambda (LambdaTree <Ptr<ReturnType>> treeWithPointer) {

		// a problem is that consumers of this lambda will not be able to deal with a Ptr<ReturnType>...
		// so we need to add an additional operation that dereferences the pointer
		std :: shared_ptr <DereferenceLambda <ReturnType>> newRoot = std :: make_shared <DereferenceLambda <ReturnType>> (treeWithPointer);
		tree = newRoot;		
	} 

	// create a lambda tree that returns a non-pointer
	Lambda (LambdaTree <ReturnType> tree) : tree (tree.getPtr ()) {}
	
	// convert one of these guys to a map
	void toMap (std :: map <std :: string, GenericLambdaObjectPtr> &returnVal, int &suffix) {
		traverse (returnVal, tree, suffix);
	}
};

}

#endif
