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
#include "MultiInputsBase.h"
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

        void getInputs ( std :: vector <std :: string>& allInputs, GenericLambdaObjectPtr root, MultiInputsBase * multiInputsBase) {
            for (int i = 0; i < root->getNumChildren(); i++) {
                GenericLambdaObjectPtr child = root->getChild (i);
                getInputs(allInputs, child, multiInputsBase);    
            }
            if (root->getNumChildren() == 0) {
                for (int i = 0; i < root->getNumInputs(); i++) {
                   std :: string myName = multiInputsBase->getNameForIthInput(root->getInputIndex(i));
                   auto iter = std :: find(allInputs.begin(), allInputs.end(), myName);
                   if (iter == allInputs.end()) {
                       allInputs.push_back(myName);
                   }
               }
            }
        }


        //JiaNote: below function is to generate a sequence of TCAP Strings for this Lambda tree
        static void getTCAPString (std :: vector <std :: string> &tcapStrings, std :: vector<std :: string> &inputTupleSetNames, std :: vector<std :: string> &inputColumnNames, std :: vector<std :: string> &inputColumnsToApply, std :: vector<std :: string> & childrenLambdaNames, GenericLambdaObjectPtr root, int &lambdaLabel, std :: string computationName, int computationLabel, std :: string & addedOutputColumnName, std :: string & myLambdaName, std :: string & outputTupleSetName, MultiInputsBase * multiInputsComp = nullptr, bool amIPartOfJoinPredicate = false, bool amILeftChildOfEqualLambda = false, bool amIRightChildOfEqualLambda = false, std :: string parentLambdaName = "") {
                std :: cout << "LambdaTree inner: " << std :: endl;
                std :: vector <std :: string> columnsToApply;
                std :: vector <std :: string> childrenLambdas;
                std :: vector <std :: string> inputNames;
                std :: vector <std :: string> inputColumns;
                if (root->getNumChildren() > 0) {
                        for (int i = 0; i < inputColumnsToApply.size(); i++) {

                                columnsToApply.push_back(inputColumnsToApply[i]);
                                std :: cout << "inputColumnsToApply[" << i << "]=" << inputColumnsToApply[i] << std :: endl;
                        }
                        inputColumnsToApply.clear();
                        for (int i = 0; i < childrenLambdaNames.size(); i++) {
                                childrenLambdas.push_back(childrenLambdaNames[i]);
                        }
                        childrenLambdaNames.clear();
                        for (int i = 0; i < inputTupleSetNames.size(); i++) {
                                auto iter = std :: find(inputNames.begin(), inputNames.end(), inputTupleSetNames[i]);
                                if (iter == inputNames.end()) {
                                    inputNames.push_back(inputTupleSetNames[i]);
                                }
                        }
                        inputTupleSetNames.clear();
                        for (int i = 0; i < inputColumnNames.size(); i++) {
                                inputColumns.push_back(inputColumnNames[i]);
                                std :: cout << "inputColumnNames[" << i << "]=" << inputColumnNames[i] << std :: endl;
                        }
                        inputColumnNames.clear();
                } else {
                        for (int i = 0; i < inputColumnsToApply.size(); i++) {
                                std :: cout << "inputColumnsToApply[" << i << "]=" << inputColumnsToApply[i] << std :: endl;
                        }
                        for (int i = 0; i < inputColumnNames.size(); i++) {
                                std :: cout << "inputColumnNames[" << i << "]=" << inputColumnNames[i] << std :: endl;
                        }
                }

                std :: string myTypeName = root->getTypeOfLambda ();
                PDB_COUT << "\tExtracted lambda named: " << myTypeName << "\n";
                std :: string myName = myTypeName + "_" + std :: to_string (lambdaLabel+root->getNumChildren());
                
                bool isLeftChildOfEqualLambda = false;
                bool isRightChildOfEqualLambda = false;
                for (int i = 0; i < root->getNumChildren (); i++) {
                        GenericLambdaObjectPtr child = root->getChild (i);
                        if (myTypeName == "==") {
                            if (i == 0) {
                                isLeftChildOfEqualLambda = true;
                            }
                            if (i == 1) {
                                isRightChildOfEqualLambda = true;
                            }
                        }        
                        getTCAPString (tcapStrings, inputNames, inputColumns, columnsToApply, childrenLambdas, child, lambdaLabel, computationName, computationLabel, addedOutputColumnName, myLambdaName, outputTupleSetName, multiInputsComp, amIPartOfJoinPredicate, isLeftChildOfEqualLambda, isRightChildOfEqualLambda, myName);
                        inputColumnsToApply.push_back(addedOutputColumnName);
                        childrenLambdaNames.push_back(myLambdaName);
                        if (multiInputsComp != nullptr) {
                            auto iter = std :: find(inputTupleSetNames.begin(), inputTupleSetNames.end(), outputTupleSetName);
                            if (iter == inputTupleSetNames.end()) {
                                inputTupleSetNames.push_back(outputTupleSetName);
                            }
                        } else {
                             inputTupleSetNames.clear();
                             inputTupleSetNames.push_back(outputTupleSetName);
                             inputColumnNames.clear();
                        }
                        for (int j = 0; j < inputColumns.size(); j++) {
                             inputColumnNames.push_back(inputColumns[j]);
                        }
                        isLeftChildOfEqualLambda = false;
                        isRightChildOfEqualLambda = false;
                }
                std :: vector<std :: string> outputColumns;
                std :: string tcapString = root->toTCAPString(inputTupleSetNames, inputColumnNames, inputColumnsToApply, childrenLambdaNames, lambdaLabel, computationName, computationLabel, outputTupleSetName, outputColumns, addedOutputColumnName, myLambdaName, multiInputsComp, amIPartOfJoinPredicate, amILeftChildOfEqualLambda, amIRightChildOfEqualLambda, parentLambdaName);
                std :: cout << tcapString << std :: endl;
                tcapStrings.push_back(tcapString);
                lambdaLabel++;
                if (multiInputsComp == nullptr) {
                        inputTupleSetNames.clear();
                        inputTupleSetNames.push_back(outputTupleSetName);
                }
                inputColumnNames.clear();
                for (int i = 0; i < outputColumns.size(); i++) {
                        inputColumnNames.push_back(outputColumns[i]);
                }

        }

public:

        unsigned int getInputIndex() {
            return tree->getInputIndex();
        }

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


        std :: vector < std :: string> getAllInputs ( MultiInputsBase * multiInputsBase) {
             std :: cout << "All inputs in this lambda tree:" << std :: endl;
             std :: vector < std :: string > ret;
             this->getInputs(ret, tree, multiInputsBase);
             for (int i = 0; i < ret.size(); i++) {
                 std :: cout << ret[i] << std :: endl;
             }
             return ret;
        }

        //to get the TCAPString for this lambda tree
        std :: string toTCAPString(std :: string inputTupleSetName, std :: vector<std :: string> inputColumnNames, std :: vector<std :: string> inputColumnsToApply, std :: vector <std :: string> childrenLambdaNames, int &lambdaLabel, std :: string computationName, int computationLabel, std :: string & outputTupleSetName, std :: vector<std :: string> & outputColumnNames, std :: string & addedOutputColumnName, std :: string & myLambdaName, bool whetherToRemoveUnusedOutputColumns, MultiInputsBase * multiInputsComp = nullptr, bool amIPartOfJoinPredicate = false) {
            std :: cout << "LambdaTree: outer:" << std :: endl;
            std :: vector<std :: string> tcapStrings;
            std :: string outputTCAPString;
            std :: vector<std :: string> inputTupleSetNames;
            inputTupleSetNames.push_back(inputTupleSetName);
            std :: vector<std :: string> columnNames;
            for (int i = 0; i < inputColumnNames.size(); i ++) {
                    std :: cout << "inputColumnNames[" << i << "]=" << inputColumnNames[i] << std :: endl;
                    columnNames.push_back(inputColumnNames[i]);
            }
            std :: vector<std :: string> columnsToApply;
            for (int i = 0; i < inputColumnsToApply.size(); i ++) {
                    std :: cout << "inputColumnsToApply[" << i << "]=" << inputColumnsToApply[i] << std :: endl;
                    columnsToApply.push_back(inputColumnsToApply[i]);
            }
            std :: vector<std :: string> childrenLambdas;
            for (int i = 0; i < childrenLambdaNames.size(); i++) {
                    childrenLambdas.push_back(childrenLambdaNames[i]);
            }
            getTCAPString (tcapStrings, inputTupleSetNames, columnNames, columnsToApply, childrenLambdas, tree, lambdaLabel, computationName, computationLabel, addedOutputColumnName, myLambdaName, outputTupleSetName, multiInputsComp, amIPartOfJoinPredicate);
            PDB_COUT << "Lambda: lambdaLabel=" << lambdaLabel << std :: endl;
            bool isOutputInInput = false;
            outputColumnNames.clear();
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
