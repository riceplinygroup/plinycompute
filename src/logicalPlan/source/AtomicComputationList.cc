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

#ifndef COMP_LIST_CC
#define COMP_LIST_CC

#include "AtomicComputationList.h"
#include "AtomicComputationClasses.h"
#include "PDBDebug.h"

// gets the computation that builds the tuple set with the specified name
AtomicComputationPtr AtomicComputationList :: getProducingAtomicComputation (std :: string outputName) {
	if (producers.count (outputName) == 0) {
		PDB_COUT << "This could be bad... can't find the guy producing output " << outputName << ".\n";
	}
	return producers [outputName];
}

// gets the list of comptuations that consume the tuple set with the specified name
std :: vector <AtomicComputationPtr> &AtomicComputationList :: getConsumingAtomicComputations (std :: string inputName) {
	if (consumers.count (inputName) == 0) {
		PDB_COUT << "This could be bad... can't find the guy consuming input " << inputName << ".\n";
	}
	return consumers [inputName];
}

// this effectively gets all of the leaves of the graph, since it returns all of the scans... every
// AtomicComputationPtr in the returned list will point to a ScanSet object
std :: vector <AtomicComputationPtr> &AtomicComputationList :: getAllScanSets () {
	return scans;
}

// add an atomic computation to the graph
void AtomicComputationList :: addAtomicComputation (AtomicComputationPtr addMe) {

	if (addMe->getAtomicComputationType () == "Scan") {
		scans.push_back (addMe);
	}

	producers[addMe->getOutputName ()] = addMe;
	if (consumers.count (addMe->getInputName ()) == 0) {
		std :: vector <AtomicComputationPtr> rhs;
		consumers[addMe->getInputName ()] = rhs;
	}
	consumers[addMe->getInputName ()].push_back (addMe);

	// now, see if this guy is a join; join is special, because we have to add both inputs to the 
	// join to the consumers map
	if (addMe->getAtomicComputationType () == "JoinSets") {
		ApplyJoin *myPtr = (ApplyJoin *) addMe.get ();
		consumers[myPtr->getRightInput ().getSetName ()].push_back (addMe);	
	}

	// kill the copy of the shared pointer that is inside him
	addMe->destroyPtr ();
}

std :: ostream& operator<<(std :: ostream& os, const AtomicComputationList& printMe) {
	for (auto &a : printMe.producers) {
		os << a.second->output << " <= " << a.second->getAtomicComputationType () << 
			"(" << a.second->input << ", " << a.second->projection;
		os << ")\n";
	}
	return os;
}

#endif
