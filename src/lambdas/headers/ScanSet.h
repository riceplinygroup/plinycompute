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

#ifndef SCAN_SET_H
#define SCAN_SET_H

#include "Computation.h"

namespace pdb {

template <class OutputClass>
class ScanSet : public Computation {

	std :: string getComputationType () override {
		return std :: string ("ScanSet");
	}


        // gets the name of the i^th input type...
        std :: string getIthInputType (int i) override {
                return "";
        }

        // get the number of inputs to this query type
        int getNumInputs() override {
                return 0;
        }

        // gets the output type of this query as a string
        std :: string getOutputType () override {
                return getTypeName<OutputClass>();
        }

        bool needsMaterializeOutput () override {
                return false;
        }

        

        // below function implements the interface for parsing computation into a TCAP string
        std :: string toTCAPString (std :: vector <InputTupleSetSpecifier>& inputTupleSets, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string>& outputColumnNames, std :: string& addedOutputColumnName) override {

    InputTupleSetSpecifier inputTupleSet;
    if (inputTupleSets.size() > 0) {
        inputTupleSet = inputTupleSets[0];
    }
    return toTCAPString (inputTupleSet.getTupleSetName(), inputTupleSet.getColumnNamesToKeep(), inputTupleSet.getColumnNamesToApply(), computationLabel, outputTupleSetName, outputColumnNames, addedOutputColumnName);
 }

        // below function returns a TCAP string for this Computation
        std :: string toTCAPString (std :: string inputTupleSetName, std :: vector<std :: string>& inputColumnNames, std :: vector<std :: string>& inputColumnsToApply, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string>& outputColumnNames, std :: string& addedOutputColumnName)  {

                addedOutputColumnName = "in"+std :: to_string(computationLabel);
                outputTupleSetName = "inputData"+std :: to_string(computationLabel);
                std :: string ret = outputTupleSetName + "(" + addedOutputColumnName + std :: string(") <= SCAN ('") + std :: string("empty()") + "', '" + std :: string("empty()") + std :: string("', '") + getComputationType() + std :: string("_") + std :: to_string(computationLabel) + std :: string("')\n");
                outputColumnNames.push_back(addedOutputColumnName);
                this->setTraversed (true);
                this->setOutputTupleSetName (outputTupleSetName);
                this->setOutputColumnToApply (addedOutputColumnName);
                return ret;
       }


};

}

#endif
