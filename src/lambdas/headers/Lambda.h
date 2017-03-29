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
        // JiaNote: I changed below method from pre-order traversing to post-order traversing, so that it follows the lambda execution ordering
	static void traverse (std :: map <std :: string, GenericLambdaObjectPtr> &fillMe, 
		GenericLambdaObjectPtr root, int &startLabel) {

		for (int i = 0; i < root->getNumChildren (); i++) {
			GenericLambdaObjectPtr child = root->getChild (i);
			traverse (fillMe, child, startLabel);
                }
                std :: string myName = root->getTypeOfLambda ();
                myName = myName + "_" + std :: to_string (startLabel);
                std :: cout << "\tExtracted lambda named: " << myName << "\n";
                startLabel++;
                fillMe[myName] = root;
	}


        //JiaNote: below function is to generate a sequence of TCAP Strings for this Lambda tree
        static void getTCAPString (std :: vector <std :: string> &tcapStrings, std :: string &inputTupleSetName, std :: vector<std :: string> &inputColumnNames, std :: vector<std :: string> &inputColumnsToApply, GenericLambdaObjectPtr root, int &lambdaLabel, std :: string computationName, int computationLabel, std :: string & addedOutputColumnName) {

                std :: vector <std :: string> columnsToApply;
                if (root->getNumChildren() > 0) {
                        for (int i = 0; i < inputColumnsToApply.size(); i++) {
                                columnsToApply.push_back(inputColumnsToApply[i]);
                        }
                        inputColumnsToApply.clear();
                }
                for (int i = 0; i < root->getNumChildren (); i++) {
                        GenericLambdaObjectPtr child = root->getChild (i);
                        getTCAPString (tcapStrings, inputTupleSetName, inputColumnNames, columnsToApply, child, lambdaLabel, computationName, computationLabel, addedOutputColumnName);
                        inputColumnsToApply.push_back(addedOutputColumnName);
                }
                std :: string myName = root->getTypeOfLambda ();
                myName = myName + "_" + std :: to_string (lambdaLabel);
                PDB_COUT << "\tExtracted lambda named: " << myName << "\n";
                std :: string outputTupleSetName;
                std :: vector<std :: string> outputColumns;
                std :: string tcapString = root->toTCAPString(inputTupleSetName, inputColumnNames, inputColumnsToApply, lambdaLabel, computationName, computationLabel, outputTupleSetName, outputColumns, addedOutputColumnName);
                tcapStrings.push_back(tcapString);
                inputTupleSetName = outputTupleSetName;
                inputColumnNames.clear();
                for (int i = 0; i < outputColumns.size(); i++) {
                         inputColumnNames.push_back(outputColumns[i]);
                }
                lambdaLabel++;

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

        //to get the TCAPString for this lambda tree
        std :: string toTCAPString(std :: string inputTupleSetName, std :: vector<std :: string> inputColumnNames, std :: vector<std :: string> inputColumnsToApply, int &lambdaLabel, std :: string computationName, int computationLabel, std :: string & outputTupleSetName, std :: vector<std :: string> & outputColumnNames, std :: string & addedOutputColumnName, bool whetherToRemoveUnusedOutputColumns) {
            std :: vector<std :: string> tcapStrings;
            std :: string outputTCAPString;
            std :: string tupleSetName = inputTupleSetName;
            std :: vector<std :: string> columnNames;
            for (int i = 0; i < inputColumnNames.size(); i ++) {
                    columnNames.push_back(inputColumnNames[i]);
            }
            std :: vector<std :: string> columnsToApply;
            for (int i = 0; i < inputColumnsToApply.size(); i ++) {
                    columnsToApply.push_back(inputColumnsToApply[i]);
            }
            getTCAPString (tcapStrings, tupleSetName, columnNames, columnsToApply, tree, lambdaLabel, computationName, computationLabel, addedOutputColumnName);
            PDB_COUT << "Lambda: lambdaLabel=" << lambdaLabel << std :: endl;
            outputTupleSetName = tupleSetName;
            bool isOutputInInput = false;
            if (whetherToRemoveUnusedOutputColumns == false) { 
                for (int i = 0; i < columnNames.size(); i ++) {
                    outputColumnNames.push_back(columnNames[i]);
                    if (addedOutputColumnName == columnNames[i]) {
                        isOutputInInput = true;
                    }
                }
                if (isOutputInInput == false) {
                     outputColumnNames.push_back(addedOutputColumnName);
                }
            } else {
                outputColumnNames.push_back(addedOutputColumnName);
            }

            if (whetherToRemoveUnusedOutputColumns == true) {
                int last = tcapStrings.size()-1;
                PDB_COUT << "tcapStrings[" << last << "]=" << tcapStrings[last] << std :: endl;
                std :: string right = tcapStrings[last].substr(tcapStrings[last].find("<="));
                int pos1 = right.find_last_of("(");
                int pos2 = right.rfind("),");
                right.replace(pos1+1, pos2-1-(pos1+1)+1, "");
                tcapStrings[last] = outputTupleSetName + " (" + addedOutputColumnName + ") " + right;

            } 
            for (int i = 0; i < tcapStrings.size(); i ++) {
                outputTCAPString = outputTCAPString + tcapStrings[i];
            }
            return outputTCAPString;
        }


};

}

#endif
