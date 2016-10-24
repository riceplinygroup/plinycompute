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
#include "Literal.h"
#include "Object.h"
#include "Handle.h"
#include "Ptr.h"
#include "ParserTypes.h"
#include "QueryExecutor.h"

namespace pdb {

// The basic idea is that we have a "class Lambda <typename Out>" that is returned by the query object.

// Internally, the query object creates a "class LambdaTree <Out>" object.  The reason that the
// query internally constructs a "class LambdaTree <Out>" whereas the query returns a 
// "class Lambda <Out>" is that there may be a mismatch between the two type parameters---the 
// LambdaTree may return a "class LambdaTree <Ptr<Out>>" object for efficiency.  Thus, we allow 
// a "class Lambda <Out>" object to be constructed with either a  "class LambdaTree <Out>"
// or a  "class LambdaTree <Ptr<Out>>".  We don't want to allow implicit conversions between
//  "class LambdaTree <Out>" and "class LambdaTree <Ptr<Out>>", however, which is why we need
// the separate type.

// Each "class LambdaTree <Out>" object is basically just a wrapper for a shared_ptr to a
// "TypedLambdaObject <Out> object".  So that we can pass around pointers to these things (without
// return types), "TypedLambdaObject <Out>" derives from "GenericLambdaObject". 

// forward delcaration
template <typename Out>
class TypedLambdaObject;

// we wrap up a shared pointer (rather than using a simple shared pointer) so that we 
// can override operations on these guys (if we used raw shared pointers, we could not)
template <typename ReturnType>
class LambdaTree {

private:

	std :: shared_ptr <TypedLambdaObject <ReturnType>> me;

public:

	LambdaTree () {}

	auto &getPtr () {
		return me;
	}

	LambdaTree <ReturnType> *operator -> () const {
		return me.get ();
	}

	LambdaTree <ReturnType> &operator * () const {
		return *me;
	}
	
	template <class Type> 
	LambdaTree (std :: shared_ptr <Type> meIn) {
		me = meIn;	
	}

	LambdaTree (const LambdaTree <ReturnType> &toMe) : me (toMe.me) {}

	LambdaTree <ReturnType> &operator = (const LambdaTree <ReturnType> &toMe) {
		me = toMe.me;
		return *this;
	}	

	template <class Type>
	LambdaTree <ReturnType> &operator = (std :: shared_ptr <Type> toMe) {
		me = toMe;
		return *this;
	}	
};

class GenericLambdaObject;
typedef std :: shared_ptr <GenericLambdaObject> GenericLambdaObjectPtr;

// this is the base class from which all pdb :: Lambdas derive
class GenericLambdaObject {

private:

	std :: vector <Handle <Object> *> inputs;

public:

	// this gets an executor that appends the result of running this lambda to the end of each tuple
	virtual QueryExecutorPtr getExecutor (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput) = 0;
	
	// this will return a nullptr unless this lambda implements an equals
	// otherwise, gets a query executor that appends a hash of an attribute to the end of each tuple
	virtual QueryExecutorPtr getHasher (TupleSpec &inputSchema, TupleSpec &attToOperateOn, TupleSpec &attsToIncludeInOutput) = 0;

	// returns the name of this LambdaBase type, as a string
	virtual std :: string getTypeOfLambda () = 0;

	// returns the number of children of this Lambda type
	virtual int getNumChildren () = 0;

	// gets a particular child of this Lambda
	virtual GenericLambdaObjectPtr getChild (int which) = 0;

	// gets Handles that this guy was bound to
	std :: vector <Handle <Object> *> &getBoundInputs () {
		return inputs;
	}
	
	// returns a string containing the type that is returned when this lambda is executed
	virtual std :: string getOutputType () = 0;

	virtual ~GenericLambdaObject () {}
};

// this is the lamda type... queries are written by supplying code that
// creates these objects
template <typename Out>
class TypedLambdaObject : public GenericLambdaObject {

	public:

		std :: string getOutputType () override {
			return getTypeName <Out> ();
		}

		virtual ~TypedLambdaObject () {}
};

template <class ReturnType>
class Lambda {

private:

	// in case we wrap up a non-pointer type
	std :: shared_ptr <TypedLambdaObject <ReturnType>> tree;

	// in case we wrap up a pointer type
	std :: shared_ptr <TypedLambdaObject <Ptr<ReturnType>>> treeWithPointer;

	// does the actual tree traversal
	static void traverse (std :: map <std :: string, GenericLambdaObjectPtr> &fillMe, 
		GenericLambdaObjectPtr root, int &startLabel) {

		std :: string myName = root->getTypeOfLambda ();		
		myName = myName + "_" + std :: to_string (startLabel);
		startLabel++;
		fillMe[myName] = root;
		for (int i = 0; i < root->getNumChildren (); i++) {
			GenericLambdaObjectPtr child = root->getChild (i);
			traverse (fillMe, child, startLabel);
		}	
	}

public:

	// create a lambda tree that returns a pointer
	Lambda (LambdaTree <Ptr<ReturnType>> treeWithPointer) : treeWithPointer (treeWithPointer.getPtr ()) {}

	// create a lambda tree that returns a non-pointer
	Lambda (LambdaTree <ReturnType> tree) : tree (tree.getPtr ()) {}
	
	// convert one of these guys to a map
	void toMap (std :: map <std :: string, GenericLambdaObjectPtr> &returnVal, int &suffix) {
		
		if (tree != nullptr) {
			traverse (returnVal, tree, suffix);
		} else if (treeWithPointer != nullptr) {
			traverse (returnVal, treeWithPointer, suffix);
		}
	}
};

}

#endif
