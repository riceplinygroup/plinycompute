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

#ifndef SELECTION_COMP
#define SELECTION_COMP

#include "Computation.h"
#include "TypeName.h"

namespace pdb {

template <class OutputClass, class InputClass>
class SelectionComp : public Computation {

	// the computation returned by this method is called to see if a data item should be returned in the output set
	virtual Lambda <bool> getSelection (Handle <InputClass> &checkMe) = 0;

	// the computation returned by this method is called to perfom a transformation on the input item before it
	// is inserted into the output set
	virtual Lambda <Handle<OutputClass>> getProjection (Handle <InputClass> &checkMe) = 0;

	// calls getProjection and getSelection to extract the lambdas
	void extractLambdas (std :: map <std :: string, GenericLambdaObjectPtr> &returnVal) override {
		int suffix = 0;
		Handle <InputClass> checkMe = nullptr;
		Lambda <bool> selectionLambda = getSelection (checkMe);
		Lambda <Handle <OutputClass>> projectionLambda = getProjection (checkMe);
		selectionLambda.toMap (returnVal, suffix);
		projectionLambda.toMap (returnVal, suffix);
	}	

	// this is a selection computation
        std :: string getComputationType () override {
                return std :: string ("SelectionComp");
        }

        // gets the name of the i^th input type...
        std :: string getIthInputType (int i) override {
                if ( i == 0) {
                    return getTypeName<InputClass>();
                } else {
                    return "";
                }
        }

        // get the number of inputs to this query type
        int getNumInputs() override {
                return 1;
        }

        // gets the output type of this query as a string
        std :: string getOutputType () override {
                return getTypeName<OutputClass>();
        }

        // below function implements the interface for parsing computation into a TCAP string
        std :: string toTCAPString (std :: vector <InputTupleSetSpecifier> inputTupleSets, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string>& outputColumnNames, std :: string& addedOutputColumnName) override {
              
    if (inputTupleSets.size() == 0) {
        return "";
    }
    InputTupleSetSpecifier inputTupleSet = inputTupleSets[0];
    return toTCAPString (inputTupleSet.getTupleSetName(), inputTupleSet.getColumnNamesToKeep(), inputTupleSet.getColumnNamesToApply(), computationLabel, outputTupleSetName, outputColumnNames, addedOutputColumnName);
 } 


        // to return Selection tcap string
        std :: string toTCAPString (std :: string inputTupleSetName, std :: vector<std :: string> inputColumnNames, std :: vector<std :: string> inputColumnsToApply, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string>& outputColumnNames, std :: string& addedOutputColumnName)  {
                PDB_COUT << "To GET TCAP STRING FOR SELECTION" << std :: endl;
                std :: string tcapString = "";
                Handle<InputClass> checkMe = nullptr;
                PDB_COUT << "TO GET TCAP STRING FOR SELECTION LAMBDA" << std :: endl;
                Lambda <bool> selectionLambda = getSelection (checkMe);
                std :: string tupleSetName;
                std :: vector<std :: string> columnNames;
                std :: string addedColumnName;
                int lambdaLabel = 0;
                tcapString += selectionLambda.toTCAPString(inputTupleSetName, inputColumnNames, inputColumnsToApply, lambdaLabel, getComputationType(), computationLabel, tupleSetName, columnNames, addedColumnName, false);
                PDB_COUT << "tcapString after parsing selection lambda: " << 
tcapString << std :: endl;
                PDB_COUT << "lambdaLabel=" << lambdaLabel << std :: endl;
                std :: string newTupleSetName = "filteredInputFor" + getComputationType() + std :: to_string(computationLabel);
                tcapString += newTupleSetName + "(" + inputColumnNames[0];
                for (int i = 1; i < inputColumnNames.size(); i++) {
                    tcapString += ", " + inputColumnNames[i];
                }
                tcapString += ") <= FILTER (" + tupleSetName + "(" + addedColumnName + "), " + tupleSetName + "(" + inputColumnNames[0];
                for (int i = 1; i < inputColumnNames.size(); i++) {
                    tcapString += ", ";
                    tcapString += inputColumnNames[i];
                }
                tcapString += ")";
                tcapString += ", '" + getComputationType() + "_" + std :: to_string (computationLabel) + "')\n";
                PDB_COUT << "tcapString after adding filter operation: " << tcapString << std :: endl;
                Lambda <Handle<OutputClass>> projectionLambda = getProjection (checkMe);
                PDB_COUT << "TO GET TCAP STRING FOR PROJECTION LAMBDA" << std :: endl;
                PDB_COUT << "lambdaLabel=" << lambdaLabel << std :: endl;
                tcapString += projectionLambda.toTCAPString(newTupleSetName, inputColumnNames, inputColumnsToApply, lambdaLabel, getComputationType(), computationLabel, outputTupleSetName, outputColumnNames, addedOutputColumnName, true);
                return tcapString;
        }

};

}

#endif
