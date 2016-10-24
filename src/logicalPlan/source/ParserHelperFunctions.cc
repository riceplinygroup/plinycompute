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

#ifndef SQL_PARSER_C
#define SQL_PARSER_C

#include <iostream>
#include <stdlib.h>
#include "Parser.h"
#include "ParserHelperFunctions.h"
#include "ParserTypes.h"
#include <string>
#include <map>
#include <vector>
#include <utility>

/*************************************************/
/** HERE WE DEFINE ALL OF THE C FUNCTIONS TO    **/
/** MANIPULATE THE ABOVE DATA TYPES             **/
/*************************************************/

extern "C" {

struct AttList *makeAttList (char *fromMe) {
	AttList *returnVal = new AttList ();
	returnVal->appendAttribute (fromMe);
	free (fromMe);
	return returnVal;
}

struct AttList *pushBackAttribute (struct AttList *addToMe, char *fromMe) {
	addToMe->appendAttribute (fromMe);
	free (fromMe);
	return addToMe;
}

struct TupleSpec *makeTupleSpec (char *setName, struct AttList *useMe) {
	TupleSpec *returnVal = new TupleSpec (std :: string (setName), *useMe);
	delete useMe;
	return returnVal;
}

struct Computation *makeFilter (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection) {
	std :: cout << *output << " " << *input << " " << *projection << "\n";
	ComputationPtr returnVal = std :: make_shared <ApplyFilter> (*input, *output, *projection);;
	returnVal->setShared (returnVal);
	delete output;
	delete input;
	delete projection;
	return returnVal.get ();
}

struct Computation *makeApply (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *name) {
	ComputationPtr returnVal = std :: make_shared <ApplyLambda> (*input, *output, *projection, std :: string (name));
	returnVal->setShared (returnVal);
	delete output;
	delete input;
	delete projection;
	return returnVal.get ();
}

struct ComputationList *makeComputationList (struct Computation *fromMe) {
	struct ComputationList *returnVal = new ComputationList ();
	returnVal->addComputation (fromMe->getShared ());
	return returnVal;
}

struct Input *makeInput (struct TupleSpec *output, char *dbName, char *setName) {
	struct Input *returnVal = new Input (*output, std :: string (dbName), std :: string (setName));
	free (dbName);
	free (setName);
	delete output;
	return returnVal;
}

struct InputList *makeInputList (struct Input *fromMe) {
	struct InputList *returnVal = new InputList ();
	returnVal->addInput (*fromMe);
	delete fromMe;
	return returnVal;
}

struct InputList *pushBackInput (struct InputList *intoMe, struct Input *pushMe) {
	intoMe->addInput (*pushMe);
	delete pushMe;
	return intoMe;
}

struct OutputList *makeOutputList (struct Output *fromMe) {
	struct OutputList *returnVal = new OutputList ();
	returnVal->addOutput (*fromMe);
	delete fromMe;
	return returnVal;
}

struct OutputList *pushBackOutput (struct OutputList *intoMe, struct Output *pushMe) {
	intoMe->addOutput (*pushMe);
	delete pushMe;
	return intoMe;
}

struct LogicalPlan *makePlan (struct OutputList *outputs, struct InputList *inputs, struct ComputationList *computations) {
	struct LogicalPlan *returnVal = new LogicalPlan (*outputs, *inputs, *computations);
	return returnVal;
}

struct ComputationList *pushBackComputation (struct ComputationList *input, struct Computation *addMe) {
	input->addComputation (addMe->getShared ());
	return input;
}

struct Output *makeOutput (struct TupleSpec *fromMe, char *dbName, char *setName) {
	struct Output *returnVal = new Output (*fromMe, std :: string (dbName), std :: string (setName));
	free (dbName);
	free (setName);
	delete fromMe;
	return returnVal;
}

// structure that stores a list of aliases from a FROM clause
} // extern

#endif
