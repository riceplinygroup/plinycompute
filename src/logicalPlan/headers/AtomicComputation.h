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

#ifndef ATOMIC_COMPUTATION_H
#define ATOMIC_COMPUTATION_H

#include <iostream>
#include <memory>
#include <stdlib.h>
#include <string>
#include <utility>
#include <vector>
#include <map>

#include "TupleSpec.h"

struct AtomicComputationList;

// all computations in TCAP (aggregates, scans, filters, etc.) descend from this class
struct AtomicComputation;
typedef std :: shared_ptr <struct AtomicComputation> AtomicComputationPtr;

struct AtomicComputation {

private:

	TupleSpec input;
	TupleSpec output;
	TupleSpec projection;
	std :: string computationName;
	AtomicComputationPtr me;

public:

	// returns the type of this computation
	virtual std :: string getAtomicComputationType () = 0;

	// sometimes, we'll need to figure out the type of a particular attribute in a tuple set.  What this does is to
	// compute, for a particular attribute in the output of the AtomicComputation, what (computationName, lambdaName) 
	// pair is that is responsible for creating this attribute... then, we can ask that pair what the type of the
	// atribute is. 
	virtual std :: pair <std :: string, std :: string> findSource (std :: string attName, AtomicComputationList &allComps) = 0;

	// virtual destructor
	virtual ~AtomicComputation () {}

	// get a shared pointer to this computation..
	AtomicComputationPtr getShared () {
		return me;
	}

	// forget the shared poitner for this computation
	void destroyPtr () {
		me = nullptr;
	}

	// simple constructor... gives the tuple specs that this guy (a) accepts as input, (b) produces as output, and (c)
	// projects from the input to perform the computation.  It also accepts the name of the Computation object that
	// is actually responsible for this computation
	AtomicComputation (TupleSpec inputIn, TupleSpec outputIn, TupleSpec projectionIn, std :: string computationName) : 
		input (inputIn), output (outputIn), projection (projectionIn), computationName (computationName) {}
	
	// remember the shared pointer for this computation
	void setShared (AtomicComputationPtr meIn) {
		me = meIn;
	}

	// gets the tuple set specifier for the output of this computation
	TupleSpec &getOutput () {
		return output;
	}	

	// gets the name of the tuple set produced by this computation
	std :: string &getOutputName () {
		return output.getSetName ();
	}

	// gets the specifier for the input tuple set used by this computation
	TupleSpec &getInput () {
		return input;
	}	

	// gets the name of the tuple set operated on by this computation
	std :: string &getInputName () {
		return input.getSetName ();
	}

	// gets the specifier for the set of output attributes that will be produced by this computation
	TupleSpec &getProjection () {
		return projection;
	}

	// this gets a string that allows us to look up the actual Computation object associated wit this node
	std :: string &getComputationName () {
		return computationName;
	}

	// for printing
	friend std :: ostream& operator<<(std :: ostream& os, const AtomicComputationList& printMe);

	// this finds the position of the specified attribute in all of the output attributes
	int findPosInOutputAtts (std :: string &findMe) {
                // find where the attribute appears in the outputs
                int counter = 0;
                for (auto &a : getOutput ().getAtts ()) {
                        if (a == findMe) {
                                break;
                        }
                        counter++;
                }

		if (getOutput ().getAtts ().size () == counter) {
			std :: cout << "This is bad... could not find the attribute that you were asking for!!\n";
			exit (1);
		}

		return counter;
	}
};

#endif
