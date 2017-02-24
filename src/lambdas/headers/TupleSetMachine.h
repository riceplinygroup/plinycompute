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

#ifndef TUPLE_SET_MACHINE_H
#define TUPLE_SET_MACHINE_H

namespace pdb {

class TupleSetSetupMachine {

	std :: vector <int> matches;
	TupleSpec &inputSchema;

public:

	TupleSetSetupMachine (TupleSpec &inputSchema, TupleSpec &attsToIncludeInOutput) : inputSchema (inputSchema) {
		matches = match (attsToIncludeInOutput);
	}	
	
	// gets a vector that tells us where all of the attributes match
	std :: vector <int> match (TupleSpec &attsToMatch) {
		
		// find the positions of all of the matches
		std :: vector <int> matches;	
		for (auto &s : attsToMatch.getAtts ()) {
			int counter = 0;
			for (auto &t : inputSchema.getAtts ()) {
				if (s == t) {
					matches.push_back (counter);
					counter = -1;
					break;
				}
				counter++;
			}
			if (counter != -1) {
				std :: cout << "This is bad... could not find a matching attribute\n";
				std :: cout << "Atts to match was: " << attsToMatch << "\n";
				std :: cout << "Input schema was: " << inputSchema << "\n";
			}
		}
		return matches;
	}

	// sets up the output tuple by copying over all of the atts that we need to, and setting the output
	void setup (TupleSetPtr input, TupleSetPtr output) {
		
		// first, do a shallow copy of all of the atts that are being copied over
		int counter = 0;
		for (auto &i : matches) {

			// copy the column over, deleting the old one, if necessary
			output->copyColumn (input, i, counter++);
		}
	}

};

using TupleSetSetupMachinePtr = std :: shared_ptr <TupleSetSetupMachine>;

}

#endif
