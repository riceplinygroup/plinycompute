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

#ifndef COMP_CLASSES_H
#define COMP_CLASSES_H

#include "TupleSpec.h"

// NOTE: these are NOT part of the pdb namespace because they need to be included in an "extern C"...
// I am not sure whether that is possible... perhaps we try moving them to the pdb namespace later.

// this is a computation that applies a lambda to a tuple set
struct ApplyLambda : public AtomicComputation {

private:
	
	std :: string lambdaName;

public:

	~ApplyLambda () {}

	ApplyLambda (TupleSpec &input, TupleSpec &output, TupleSpec &projection, std :: string nodeName, std :: string lambdaNameIn) : 
		AtomicComputation (input, output, projection, nodeName), lambdaName (lambdaNameIn) {}

	std :: string getAtomicComputationType () override {
		return std :: string ("Apply");
	}

	// returns the name of the lambda we are supposed to apply
	std :: string &getLambdaToApply () {
		return lambdaName;
	}

	friend std :: ostream& operator<<(std :: ostream& os, const AtomicComputationList& printMe);
};

// this is a computation that performs a filer over a tuple set
struct ApplyFilter : public AtomicComputation {

public:

	~ApplyFilter () {}

	ApplyFilter (TupleSpec &input, TupleSpec &output, TupleSpec &projection, std :: string nodeName) : 
		AtomicComputation (input, output, projection, nodeName) {}

	std :: string getAtomicComputationType () override {
		return std :: string ("Filter");
	}

};

// this is a computation that aggregates a tuple set
struct ApplyAgg : public AtomicComputation {

public:
	
	~ApplyAgg () {}
	
	ApplyAgg (TupleSpec &input, TupleSpec &output, TupleSpec &projection, std :: string nodeName) : 
		AtomicComputation (input, output, projection, nodeName) {}

	std :: string getAtomicComputationType () override {
		return std :: string ("Aggregate");
	}

};

// this is a computation that produces a tuple set by scanning a set stored in the database
struct ScanSet : public AtomicComputation {

	std :: string dbName;
	std :: string setName;

public:

	~ScanSet () {}

	ScanSet (TupleSpec &output, std :: string dbName, std :: string setName, std :: string nodeName) : 
		AtomicComputation (TupleSpec (), output, TupleSpec (), nodeName), dbName (dbName), setName (setName) {}

	std :: string getAtomicComputationType () override {
		return std :: string ("Scan");
	}

	std :: string &getDBName () {
		return dbName;
	}

	std :: string &getSetName () {
		return setName;
	}

};

// this is a computation that writes out a tuple set
struct WriteSet : public AtomicComputation {

	std :: string dbName;
	std :: string setName;

public:

	~WriteSet () {}
	
	WriteSet (TupleSpec &input, TupleSpec &output, TupleSpec &projection, std :: string dbName, std :: string setName, std :: string nodeName) : 
		AtomicComputation (input, output, projection, nodeName), dbName (dbName), setName (setName) {}

	std :: string getAtomicComputationType () override {
		return std :: string ("WriteSet");
	}

	std :: string &getDBName () {
		return dbName;
	}

	std :: string &getSetName () {
		return setName;
	}	
};

struct ApplyJoin : public AtomicComputation {

	TupleSpec rightInput;
	TupleSpec rightProjection;

public:

	ApplyJoin (TupleSpec &output, TupleSpec &lInput, TupleSpec &lProjection,
                TupleSpec &rInput, TupleSpec &rProjection, std :: string setName, std :: string nodeName) :
		AtomicComputation (lInput, output, lProjection, nodeName), rightInput (rInput),
		rightProjection (rProjection) {}

	TupleSpec &getRightProjection () {
		return rightProjection;
	}

	TupleSpec &getRightInput () {
		return rightInput;
	}

	std :: string getAtomicComputationType () override {
		return std :: string ("JoinSets");
	}
	
};
	
#endif
