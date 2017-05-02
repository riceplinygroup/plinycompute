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

#ifndef METHOD_CALL_LAM_H
#define METHOD_CALL_LAM_H

#include <vector>
#include "Lambda.h"
#include "ComputeExecutor.h"

namespace pdb {

template <class Out, class ClassType> 
class MethodCallLambda : public TypedLambdaObject <Out> {

public:

	std::function <ComputeExecutorPtr (TupleSpec &, TupleSpec &, TupleSpec &)> getExecutorFunc;
	std::function <bool (std :: string &, TupleSetPtr, int)> columnBuilder;
	std :: string inputTypeName;
	std :: string methodName;
	std :: string returnTypeName;

public:

	// create an att access lambda; offset is the position in the input object where we are going to find the input att
	MethodCallLambda (std :: string inputTypeName, std :: string methodName, std :: string returnTypeName, Handle <ClassType> & input, 
		std::function <bool (std :: string &, TupleSetPtr, int)> columnBuilder,
		std::function <ComputeExecutorPtr (TupleSpec &, TupleSpec &, TupleSpec &)> getExecutorFunc) :
		getExecutorFunc (getExecutorFunc), columnBuilder (columnBuilder), inputTypeName (inputTypeName), 
		methodName (methodName), returnTypeName (returnTypeName) {

             std :: cout << "MethodCallLambda: input type code is " << input.getExactTypeInfoValue() << std :: endl;

	}

	/* bool addColumnToTupleSet (std :: string &typeToMatch, TupleSetPtr addToMe, int posToAddTo) override {
		return columnBuilder (typeToMatch, addToMe, posToAddTo);
	} */

	std :: string getTypeOfLambda () override {
		return std :: string ("methodCall");
	}

	std :: string whichMethodWeCall () {
		return methodName;
	}

	std :: string getInputType () {
		return inputTypeName;
	}

	std :: string getOutputType () override {
		return returnTypeName;
	}

        std :: string toTCAPString (std :: string inputTupleSetName, std :: vector<std :: string> inputColumnNames, std :: vector<std :: string> inputColumnsToApply, int lambdaLabel, std :: string computationName, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string> & outputColumns, std :: string& outputColumnName) override {
                std :: string tcapString = "";
                outputTupleSetName = "methodCall_"+std :: to_string(lambdaLabel)+"OutFor"+computationName+std :: to_string(computationLabel);

                outputColumnName = "methodCall_"+methodName;
                outputColumns.clear();
                for (int i = 0; i < inputColumnNames.size(); i++) {
                    outputColumns.push_back(inputColumnNames[i]);
                }
                outputColumns.push_back(outputColumnName);
                tcapString += outputTupleSetName + "(" + outputColumns[0];
                for (int i = 1; i < outputColumns.size(); i++) {
                    tcapString += ",";
                    tcapString += outputColumns[i];
                }
                tcapString += ") <= APPLY (";
                tcapString += inputTupleSetName + "(" + inputColumnsToApply[0];
                for (int i = 1; i < inputColumnsToApply.size(); i++) {
                    tcapString += ",";
                    tcapString += inputColumnsToApply[i];
                }
                tcapString += "), " + inputTupleSetName + "(" + inputColumnNames[0];
                for (int i = 1; i < inputColumnNames.size(); i++) {
                    tcapString += ",";
                    tcapString += inputColumnNames[i];
                }
                tcapString += "), '" + computationName + "_" + std :: to_string(computationLabel) + "', '"+ getTypeOfLambda() + "_" + std :: to_string(lambdaLabel) +"')\n";

                return tcapString;

        }



	int getNumChildren () override {
		return 0;
	}

	GenericLambdaObjectPtr getChild (int which) override {
		return nullptr;
	}

	ComputeExecutorPtr getExecutor (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput) override {
		return getExecutorFunc (inputSchema, attsToOperateOn, attsToIncludeInOutput); 
	}

};

}

#endif
