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

#ifndef LAMBDA_CREATION_H
#define LAMBDA_CREATION_H

#include "Lambda.h"
#include "Ptr.h"
#include "AttAccessLambda.h"
#include "AndLambda.h"
#include "SelfLambda.h"
#include "MethodCallLambda.h"
#include "EqualsLambda.h"
#include "SimpleComputeExecutor.h"
#include "CPlusPlusLambda.h"
#include "TypeName.h"

namespace pdb {

// these next four functions are used to create PDB Lambdas out of C++ lambdas

template <typename ParamOne, typename F>
auto makeLambda (Handle <ParamOne> pOne, F arg) {
        return LambdaTree <decltype (arg (pOne))> (std :: make_shared <CPlusPlusLambda <F, decltype (arg (pOne)), ParamOne>> (arg));
}

template <typename ParamOne, typename ParamTwo, typename F>
auto makeLambda (Handle <ParamOne> pOne, Handle <ParamTwo> pTwo, F arg) {
        return LambdaTree <decltype (arg (pOne, pTwo))> (std :: make_shared <CPlusPlusLambda <F, decltype (arg (pOne, pTwo)), ParamOne, ParamTwo>> (arg));
}

template <typename ParamOne, typename ParamTwo, typename ParamThree, typename F>
auto makeLambda (Handle <ParamOne> pOne, Handle <ParamTwo> pTwo, Handle <ParamThree> pThree, F arg) {
        return LambdaTree <decltype (arg (pOne, pTwo, pThree))> 
		(std :: make_shared <CPlusPlusLambda <F, decltype (arg (pOne, pTwo, pThree)), ParamOne, ParamTwo, ParamThree>> (arg));
}

template <typename ParamOne, typename ParamTwo, typename ParamThree, typename ParamFour, typename F>
auto makeLambda (Handle <ParamOne> pOne, Handle <ParamTwo> pTwo, Handle <ParamThree> pThree, Handle <ParamFour> pFour, F arg) {
        return LambdaTree <decltype (arg (pOne, pTwo, pThree, pFour))> 
		(std :: make_shared <CPlusPlusLambda <F, decltype (arg (pOne, pTwo, pThree, pFour)), ParamOne, ParamTwo, ParamThree, ParamFour>> (arg));
}

template <typename ParamOne, typename ParamTwo, typename ParamThree, typename ParamFour, typename ParamFive, typename F>
auto makeLambda (Handle <ParamOne> pOne, Handle <ParamTwo> pTwo, Handle <ParamThree> pThree, Handle <ParamFour> pFour, Handle <ParamFive> pFive, F arg) {
        return LambdaTree <decltype (arg (pOne, pTwo, pThree, pFour, pFive))> 
		(std :: make_shared <CPlusPlusLambda <F, decltype (arg (pOne, pTwo, pThree, pFour, pFive)), ParamOne, ParamTwo, ParamThree, ParamFour, ParamFive>> (arg));
}


// creates a PDB lambda out of an == operator
template <typename LeftType, typename RightType> 
LambdaTree <bool> operator == (LambdaTree <LeftType> lhs, LambdaTree <RightType> rhs) {
	return LambdaTree <bool> (std :: make_shared <EqualsLambda <LeftType, RightType>> (lhs, rhs));
}

// creates a PDB lambda from an && operator
template <typename LeftType, typename RightType> 
LambdaTree <bool> operator && (LambdaTree <LeftType> lhs, LambdaTree <RightType> rhs) {
	return LambdaTree <bool> (std :: make_shared <AndLambda <LeftType, RightType>> (lhs, rhs));
}

// creates a PDB lambda that simply returns the argument itself
template <typename ClassType>
LambdaTree <Ptr<ClassType>> makeLambdaFromSelf (Handle <ClassType> & var) {
        std :: cout << "makeLambdaFromSelf: input type code is " << var.getExactTypeInfoValue() << std :: endl;
	return LambdaTree <Ptr<ClassType>> (std :: make_shared <SelfLambda <ClassType>> (var));
}


// creates a PDB lambda that returns a member of a C++ class
template <typename ReturnType, typename ClassType>
LambdaTree <Ptr<ReturnType>> makeLambdaUsingMember (std :: string inputTypeName, std :: string attName, std :: string attType,
	Handle <ClassType> & var, ReturnType *member, size_t offset) {
        std :: cout << "makeLambdaUsingMember: input type code is " << var.getExactTypeInfoValue() << std :: endl;
	return LambdaTree <Ptr<ReturnType>> (std :: make_shared <AttAccessLambda <ReturnType, ClassType>> 
		(inputTypeName, attName, attType, var, offset));
}

// we have this here only so that we can cast it to be of type VAR
void *someRandomPointer;

#define makeLambdaFromMember(VAR,MEMBER) (makeLambdaUsingMember (getTypeName <std::remove_reference<decltype(*VAR)>::type> (), std :: string (#MEMBER), \
	getTypeName <typename std::remove_reference<decltype(VAR->MEMBER)>::type> (), VAR, (decltype (VAR->MEMBER) *) someRandomPointer, \
		((char *) &(((std::remove_reference<decltype(*VAR)>::type *) someRandomPointer)->MEMBER)) - (char *) someRandomPointer))

// this bit of nasty templating defines a function that creates a LambdaBase object encapsulating a method call on an underlying object.  This
// particular template accepts only methods that return references, because such a method's output is converted into a pointer (for speed)
// rather than actually copying the method's output
template <typename ReturnType, typename ClassType>
LambdaTree <std :: enable_if_t <std :: is_reference <ReturnType>::value, Ptr <typename std :: remove_reference <ReturnType> :: type>>> makeLambdaUsingMethod (
	std :: string inputTypeName, std :: string methodName, Handle <ClassType> & var, std :: string returnTypeName, ReturnType (ClassType:: *arg) (),
	std::function <bool (std :: string &, TupleSetPtr, int)> columnBuilder,
	std::function <SimpleComputeExecutorPtr (TupleSpec &, TupleSpec &, TupleSpec &)> getExecutor) {
        std :: cout << "makeLambdaFromMethod: input type code is " << var.getExactTypeInfoValue() << std :: endl;
	return LambdaTree <Ptr <typename std :: remove_reference <ReturnType> :: type>> 
		(std :: make_shared <MethodCallLambda <Ptr <typename std :: remove_reference <ReturnType> :: type>, ClassType>>
		(inputTypeName, methodName, returnTypeName, var, columnBuilder, getExecutor));
}

template <typename ReturnType, typename ClassType>
LambdaTree <std :: enable_if_t <!(std :: is_reference <ReturnType>::value), ReturnType>> makeLambdaUsingMethod (
	std :: string inputTypeName, std :: string methodName, Handle <ClassType> & var, std :: string returnTypeName, ReturnType (ClassType:: *arg) (),
	std::function <bool (std :: string &, TupleSetPtr, int)> columnBuilder,
	std::function <SimpleComputeExecutorPtr (TupleSpec &, TupleSpec &, TupleSpec &)> getExecutor) {
        std :: cout << "makeLambdaFromMethod: input type code is " << var.getExactTypeInfoValue() << std :: endl;
	return LambdaTree <ReturnType> (std :: make_shared <MethodCallLambda <ReturnType, ClassType>>
		(inputTypeName, methodName, returnTypeName, var, columnBuilder, getExecutor));
}

// called if ReturnType is a reference
template <bool B, typename InputType>
auto tryReference (InputType &arg) -> 
	typename std :: enable_if_t <B, InputType *> {
	return &arg;
}

// called if ReturnType is a reference
template <bool B, typename InputType>
auto tryReference (InputType arg) -> 
	typename std :: enable_if_t <!B, InputType *> {
	InputType *temp = nullptr;
	return temp;	
}

#define makeLambdaFromMethod(VAR,METHOD) (makeLambdaUsingMethod (getTypeName <typename std::remove_reference<decltype(*VAR)>::type> (), 		\
	std :: string (#METHOD), VAR, 															\
	getTypeName <typename std::remove_reference<decltype(*VAR)>::type> (), 										\
	&std::remove_reference<decltype(*VAR)>::type::METHOD,												\
	[] (std :: string &pleaseCreateThisType, TupleSetPtr input, int outAtt) {									\
		if (pleaseCreateThisType == getTypeName <typename std::remove_reference<decltype (VAR->METHOD ())>::type> ()) {				\
			std :: vector <typename std::remove_reference<decltype (VAR->METHOD ())>::type> *outColumn = 					\
				new std :: vector <typename std::remove_reference<decltype (VAR->METHOD ())>::type>;					\
			input->addColumn (outAtt, outColumn, true);											\
			return true;															\
		}																	\
																			\
		if (pleaseCreateThisType == getTypeName <Ptr <typename std::remove_reference<decltype (VAR->METHOD ())>::type>> ()) {			\
			std :: vector <Ptr <typename std::remove_reference<decltype (VAR->METHOD ())>::type>> *outColumn = 				\
				new std :: vector <Ptr <typename std::remove_reference<decltype (VAR->METHOD ())>::type>>;				\
			input->addColumn (outAtt, outColumn, true);											\
			return true;															\
		}																	\
																			\
		return false;																\
	},																		\
	[] (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput) {							\
																			\
		/* create the output tuple set */													\
		TupleSetPtr output = std :: make_shared <TupleSet> ();											\
																			\
		/* create the machine that is going to setup the output tuple set, using the input tuple set */						\
		TupleSetSetupMachinePtr myMachine = std :: make_shared <TupleSetSetupMachine> (inputSchema, attsToIncludeInOutput); 		\
																			\
		/* this is the input attribute that we will process */											\
		std :: vector <int> matches = myMachine->match (attsToOperateOn);									\
		int whichAtt = matches[0];														\
																			\
		/* this is the output attribute */													\
		int outAtt = attsToIncludeInOutput.getAtts ().size ();											\
																			\
		return std :: make_shared <SimpleComputeExecutor> (											\
			output,																\
			[=] (TupleSetPtr input) {													\
																			\
				/* if the method returns a reference, then we transform the method output into a pointer */				\
				if (std::is_reference<decltype (VAR->METHOD ())>::value) {								\
																			\
					/* set up the output tuple set */										\
					myMachine->setup (input, output);										\
																			\
					/* get the column to operate on... the input type is taken from the argument VAR */				\
					std :: vector <typename std::remove_reference<decltype(VAR)>::type> &inputColumn =				\
						input->getColumn <typename std::remove_reference<decltype(VAR)>::type> (whichAtt);			\
																			\
					/* setup the output column, if it is not already set up */							\
					if (!output->hasColumn (outAtt)) {										\
						std :: vector <Ptr <typename std::remove_reference<decltype (VAR->METHOD ())>::type>> *outColumn = 	\
							new std :: vector <Ptr <typename std::remove_reference<decltype (VAR->METHOD ())>::type>>;	\
						output->addColumn (outAtt, outColumn, true);								\
					}														\
																			\
					/* get the output column */											\
					std :: vector <Ptr <typename std::remove_reference<decltype (VAR->METHOD ())>::type>> &outColumn = 		\
						output->getColumn <Ptr <typename std::remove_reference<decltype (VAR->METHOD ())>::type>> (outAtt);	\
																			\
					/* loop down the column, setting the output */									\
					int numTuples = inputColumn.size ();										\
					outColumn.resize (numTuples);											\
					for (int i = 0; i < numTuples; i++) {										\
						outColumn [i] = tryReference <std::is_reference<decltype (VAR->METHOD ())>::value> (inputColumn[i]->METHOD ());	\
					}														\
					outColumn = output->getColumn <Ptr <typename std::remove_reference<decltype (VAR->METHOD ())>::type>> (outAtt); \
					return output;													\
																			\
				/* if the method does not return a reference, then we just go ahead and store a copy of what was returned */		\
				} else {														\
																			\
					/* set up the output tuple set */										\
					myMachine->setup (input, output);										\
																			\
					/* get the column to operate on... the input type is taken from the argument VAR */				\
					std :: vector <typename std::remove_reference<decltype(VAR)>::type> &inputColumn =				\
						input->getColumn <typename std::remove_reference<decltype(VAR)>::type> (whichAtt);			\
																			\
					/* setup the output column, if it is not already set up	*/							\
					if (!output->hasColumn (outAtt)) {										\
						std :: vector <typename std::remove_reference<decltype (VAR->METHOD ())>::type> *outColumn = 		\
							new std :: vector <typename std::remove_reference<decltype (VAR->METHOD ())>::type>;		\
						output->addColumn (outAtt, outColumn, true);								\
					}														\
																			\
					/* get the output column */											\
					std :: vector <typename std::remove_reference<decltype (VAR->METHOD ())>::type> &outColumn = 			\
						output->getColumn <typename std::remove_reference<decltype (VAR->METHOD ())>::type> (outAtt);		\
																			\
					/* loop down the column, setting the output */									\
					int numTuples = inputColumn.size ();										\
					outColumn.resize (numTuples);											\
					for (int i = 0; i < numTuples; i++) {										\
						outColumn [i] = inputColumn[i]->METHOD ();								\
					}														\
					return output;													\
				} 															\
			}																\
		);																	\
	}))	

}
#endif
