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

#ifndef LOG_PLAN_H
#define LOG_PLAN_H

#include <iostream>
#include <memory>
#include <stdlib.h>
#include <string>
#include <utility>
#include <vector>
#include <map>

#include "AtomicComputationList.h"
#include "ComputationNode.h"
#include "PDBVector.h"

// NOTE: this struct is not part of the pdb namspace because it needs to work with extern "C"...
// probably it can be made to work, but this is for the future :-)

// this is an actual logical plan
struct LogicalPlan {

private:

	// this is the parsed TCAP plan
	AtomicComputationList computations;

	// this allows one to access a particular Computation in the Logical Plan
	std :: map <std :: string, pdb :: ComputationNode> allConstituentComputations;
	
public:

	// getter for this guy's AtomicComputationList
	AtomicComputationList &getComputations () {
		return computations;
	}

	// constructor
	LogicalPlan (AtomicComputationList &computationsIn, pdb :: Vector <pdb :: Handle <pdb :: Computation>> &allComputations) {
		computations = computationsIn;
		std :: cout << "\nEXTRACTING LAMBDAS:\n";
		for (int i = 0; i < allComputations.size (); i++) {
			std :: string compType = allComputations[i]->getComputationType ();
			compType += "_";
			compType += std :: to_string (i);
			std :: cout << "Extracting lambdas for computation " << compType << "\n";
			pdb :: ComputationNode temp (allComputations[i]);
			allConstituentComputations[compType] = temp;			
		}
	}

	// get a particular node in the computational plan
	pdb :: ComputationNode &getNode (std :: string &whichComputationNode) {
		if (allConstituentComputations.count (whichComputationNode) == 0) {
			std :: cout << "This is bad. I did not find a node corresponding to " << whichComputationNode << "\n";
			std :: cout << "There were " << allConstituentComputations.size () << " computations.\n";
			for (auto &a : allConstituentComputations) {
				std :: cout << a.first << "\n";
			}
			exit (1);
		}
		return allConstituentComputations[whichComputationNode];
	}
	
	// getter for the list of nodes
	std :: map <std :: string, pdb :: ComputationNode> &getAllNodes () {
		return allConstituentComputations;
	}

	friend std :: ostream& operator<<(std :: ostream& os, const LogicalPlan& printMe);

	
};

inline std :: ostream& operator<<(std :: ostream& os, const LogicalPlan& printMe) {
	os << printMe.computations;
	return os;
}

#endif
