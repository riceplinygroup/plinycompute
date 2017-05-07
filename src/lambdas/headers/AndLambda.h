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

#ifndef AND_LAM_H
#define AND_LAM_H

#include <vector>
#include "Lambda.h"
#include "ComputeExecutor.h"
#include "TupleSetMachine.h"
#include "TupleSet.h"
#include "Ptr.h"

namespace pdb {

// only one of these four versions is going to work... used to automatically dereference a Ptr<blah>
// type on either the LHS or RHS of an "and" check
template <class LHS, class RHS>
std :: enable_if_t <std :: is_base_of <PtrBase, LHS> :: value && std :: is_base_of <PtrBase, RHS> :: value, bool> checkAnd (LHS lhs, RHS rhs) {
	return *lhs && *rhs;
} 

template <class LHS, class RHS>
std :: enable_if_t <std :: is_base_of <PtrBase, LHS> :: value && !(std :: is_base_of <PtrBase, RHS> :: value), bool> checkAnd (LHS lhs, RHS rhs) {
	return *lhs && rhs;
} 

template <class LHS, class RHS>
std :: enable_if_t <!(std :: is_base_of <PtrBase, LHS> :: value) && std :: is_base_of <PtrBase, RHS> :: value, bool> checkAnd (LHS lhs, RHS rhs) {
	return lhs && *rhs;
} 

template <class LHS, class RHS>
std :: enable_if_t <!(std :: is_base_of <PtrBase, LHS> :: value) && !(std :: is_base_of <PtrBase, RHS> :: value), bool> checkAnd (LHS lhs, RHS rhs) {
	return lhs && rhs;
} 

template <class LeftType, class RightType> 
class AndLambda : public TypedLambdaObject <bool> {

public:

	LambdaTree <LeftType> lhs;
	LambdaTree <RightType> rhs;

public:

	AndLambda (LambdaTree <LeftType> lhsIn, LambdaTree <RightType> rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
                std :: cout << "ANDLambda: LHS index is " << lhs.getInputIndex(0) << std :: endl;
                std :: cout << "ANDLambda: RHS index is " << rhs.getInputIndex(0) << std :: endl;
                std :: cout << "ANDLambda: LHS type is " << getTypeName<LeftType>() << std :: endl;
                std :: cout << "ANDLambda: RHS type is " << getTypeName<RightType>() << std :: endl;
                this->setInputIndex (0, lhs.getInputIndex(0));
                this->setInputIndex (1, rhs.getInputIndex(0));
	}

	std :: string getTypeOfLambda () override {
		return std :: string ("&&");
	}

        unsigned int getNumInputs() override {
            return 2;
        }

	int getNumChildren () override {
		return 2;
	}

	GenericLambdaObjectPtr getChild (int which) override {
		if (which == 0)
			return lhs.getPtr ();
		if (which == 1)
			return rhs.getPtr ();
		return nullptr;
	}



	ComputeExecutorPtr getExecutor (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput) override {
	
		// create the output tuple set
		TupleSetPtr output = std :: make_shared <TupleSet> ();

		// create the machine that is going to setup the output tuple set, using the input tuple set
		TupleSetSetupMachinePtr myMachine = std :: make_shared <TupleSetSetupMachine> (inputSchema, attsToIncludeInOutput);

		// these are the input attributes that we will process
		std :: vector <int> inputAtts = myMachine->match (attsToOperateOn);
		int firstAtt = inputAtts[0];
		int secondAtt = inputAtts[1];

		// this is the output attribute
		int outAtt = attsToIncludeInOutput.getAtts ().size ();

		return std :: make_shared <SimpleComputeExecutor> (
			output, 
			[=] (TupleSetPtr input) {

				// set up the output tuple set
				myMachine->setup (input, output);	

				// get the columns to operate on
				std :: vector <LeftType> &leftColumn = input->getColumn <LeftType> (firstAtt);
				std :: vector <RightType> &rightColumn = input->getColumn <RightType> (secondAtt);

				// create the output attribute, if needed
				if (!output->hasColumn (outAtt)) { 
					std :: vector <bool> *outColumn = new std :: vector <bool>;
					output->addColumn (outAtt, outColumn, true); 
				}

				// get the output column
				std :: vector <bool> &outColumn = output->getColumn <bool> (outAtt);

				// loop down the columns, setting the output
				int numTuples = leftColumn.size ();
				outColumn.resize (numTuples); 
				for (int i = 0; i < numTuples; i++) {
					outColumn [i] = checkAnd (leftColumn[i], rightColumn[i]);
				}
				return output;
			}
		);
		
	}


        std :: string toTCAPString (std :: vector<std :: string> inputTupleSetNames, std :: vector<std :: string> inputColumnNames, std :: vector<std :: string> inputColumnsToApply, std :: vector<std :: string> childrenLambdaNames, int lambdaLabel, std :: string computationName, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string> & outputColumns, std :: string& outputColumnName, std :: string& myLambdaName, MultiInputsBase * multiInputsComp = nullptr, bool amIPartOfJoinPredicate = false, bool amILeftChildOfEqualLambda = false, bool amIRightChildOfEqualLambda = false, std :: string parentLambdaName = "") override {

               if ((multiInputsComp != nullptr) && (amIPartOfJoinPredicate == true)) {
                   std :: string tcapString = "";
                   std :: string myComputationName = computationName + "_" + std :: to_string(computationLabel);
                   //Step 1. get list of input names in LHS
                   unsigned int leftIndex = lhs.getInputIndex(0);
                   std :: vector < std :: string > lhsColumnNames = multiInputsComp->getInputColumnsForIthInput(leftIndex);
                   std :: vector < std :: string > lhsInputNames;
                   for (unsigned int i = 0; i < lhsColumnNames.size(); i ++ ) {
                       std :: string curColumnName = lhsColumnNames[i];
                       for (int j = 0; j < multiInputsComp->getNumInputs(); j++) {
                           if (multiInputsComp->getNameForIthInput(j) == curColumnName) {
                               lhsInputNames.push_back(curColumnName);
                               break;
                           }
                       }
                   }

                   //Step 2. get list of input names in RHS
                   unsigned int rightIndex = rhs.getInputIndex(0);
                   std :: vector < std :: string> rhsColumnNames = multiInputsComp->getInputColumnsForIthInput(rightIndex);
                   std :: vector < std :: string > rhsInputNames;
                   for (unsigned int i = 0; i < rhsColumnNames.size(); i ++ ) {
                       std :: string curColumnName = rhsColumnNames[i];
                       for (int j = 0; j < multiInputsComp->getNumInputs(); j++) {
                           if (multiInputsComp->getNameForIthInput(j) == curColumnName) {
                               rhsInputNames.push_back(curColumnName);
                               break;
                           }
                       }
                   }

                   //Step 3. if two lists are disjoint do a cartesian join, otherwise return ""
                   std :: vector < std :: string> inputNamesIntersection;

                   for (unsigned int i = 0; i < lhsInputNames.size(); i++) {
                       for (unsigned int j = 0; j < rhsInputNames.size(); j++) {
                           if (lhsInputNames[i] == rhsInputNames[j]) {
                               inputNamesIntersection.push_back(lhsInputNames[i]);
                           }
                       }
                   }
                   
                   if (inputNamesIntersection.size() != 0) {
                       return "";
                   } else {
                       //we need a cartesian join
                       //hashone for lhs
                       std :: string leftTupleSetName = multiInputsComp->getTupleSetNameForIthInput(leftIndex);
                       std :: string leftColumnToApply = lhsInputNames[0];
                       std :: vector < std :: string > leftColumnsToApply;
                       leftColumnsToApply.push_back(leftColumnToApply);
                       std :: string leftOutputTupleSetName = "hashOneFor_" + leftColumnToApply;
                       std :: string leftOutputColumnName = "OneFor_left_" + std :: to_string(computationLabel) + "_" + std :: to_string(lambdaLabel);
                       std :: vector < std :: string > leftOutputColumns;
                       for (unsigned int i = 0; i < lhsColumnNames.size(); i++) {
                           leftOutputColumns.push_back(lhsColumnNames[i]);
                       }
                       leftOutputColumns.push_back(leftOutputColumnName);
                       tcapString += this->getTCAPString(leftTupleSetName, lhsColumnNames, leftColumnsToApply, leftOutputTupleSetName, leftOutputColumns, leftOutputColumnName, "HASHONE", myComputationName, "");

                       //hashone for rhs
                       std :: string rightTupleSetName = multiInputsComp->getTupleSetNameForIthInput(rightIndex);
                       std :: string rightColumnToApply = rhsInputNames[0];
                       std :: vector < std :: string > rightColumnsToApply;
                       rightColumnsToApply.push_back(rightColumnToApply);
                       std :: string rightOutputTupleSetName = "hashOneFor_" + rightColumnToApply;
                       std :: string rightOutputColumnName = "OneFor_right_" + std :: to_string(computationLabel) + "_" + std :: to_string(lambdaLabel);
                       std :: vector < std :: string > rightOutputColumns;
                       for (unsigned int i = 0; i < rhsColumnNames.size(); i++) {
                           rightOutputColumns.push_back(rhsColumnNames[i]);
                       }
                       rightOutputColumns.push_back(rightOutputColumnName);
                       tcapString += this->getTCAPString(rightTupleSetName, rhsColumnNames, rightColumnsToApply, rightOutputTupleSetName, rightOutputColumns, rightOutputColumnName, "HASHONE", myComputationName, "");

                       //cartesian join lhs and rhs
                       outputTupleSetName = "CartesianJoined_[" + lhsInputNames[0];
                       for (unsigned int i = 1; i < lhsInputNames.size(); i++) {
                           outputTupleSetName += "_" + lhsInputNames[i];
                       }
                       outputTupleSetName += "]_[" + rhsInputNames[0];
                       for (unsigned int i = 1; i < rhsInputNames.size(); i++) {
                           outputTupleSetName += "_" + rhsInputNames[i];
                       }
                       outputTupleSetName += "]";
                       outputColumns.clear();
                       tcapString += outputTupleSetName + "(" + lhsColumnNames[0];
                       outputColumns.push_back(lhsColumnNames[0]);
                       for (unsigned int i = 1; i < lhsColumnNames.size(); i++) {
                           tcapString += ", " + lhsColumnNames[i];
                           outputColumns.push_back(lhsColumnNames[i]);
                       } 
                       tcapString += ", " + rhsColumnNames[0];
                       outputColumns.push_back(rhsColumnNames[0]);
                       for (unsigned int i = 1; i < rhsColumnNames.size(); i++) {
                           tcapString += ", " + rhsColumnNames[i];
                           outputColumns.push_back(rhsColumnNames[i]);
                       }
                       tcapString +=") <= JOIN (" + leftOutputTupleSetName + "(" + leftOutputColumnName + "), " + leftOutputTupleSetName + "(" + lhsColumnNames[0];
                       for (unsigned int i = 1; i < lhsColumnNames.size(); i++) {
                           tcapString += ", " + lhsColumnNames[i];
                       }
                       tcapString += "), " + rightOutputTupleSetName + "(" + rightOutputColumnName + "), " + rightOutputTupleSetName + "(" + rhsColumnNames[0];
                       for (unsigned int i = 1; i < rhsColumnNames.size(); i++) {
                           tcapString += ", " + rhsColumnNames[i];
                       }
                       tcapString += "), '" + myComputationName + "')\n";

                       //update multiInputsComp
                       for (unsigned int i = 0; i < multiInputsComp->getNumInputs(); i++) {
                           std :: string curInput = multiInputsComp->getNameForIthInput(i);
                           auto iter = std :: find (outputColumns.begin(), outputColumns.end(), curInput);
                           if (iter != outputColumns.end()) {
                               multiInputsComp->setTupleSetNameForIthInput(i, outputTupleSetName);
                               multiInputsComp->setInputColumnsForIthInput(i, outputColumns);
                               multiInputsComp->setInputColumnsToApplyForIthInput(i, outputColumns);
                           }
                       }
                       return tcapString;

                    }

               } else {
                   return "";
               }


        }



};

}

#endif
