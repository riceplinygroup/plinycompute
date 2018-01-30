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
#include "AttList.h"
#include "KeyValueList.h"
#include "TupleSpec.h"
#include "AtomicComputationList.h"
#include "AtomicComputationClasses.h"
#include <string>
#include <map>
#include <vector>
#include <utility>

/*************************************************/
/** HERE WE DEFINE ALL OF THE C FUNCTIONS TO    **/
/** MANIPULATE THE ABOVE DATA TYPES             **/
/*************************************************/

extern "C" {

// ss107: Newer functions for updating TCAP:
struct KeyValueList *makeEmptyKeyValueList () {
	KeyValueList *returnVal = new KeyValueList ();
	return returnVal;
}

struct KeyValueList *makeKeyValueList (char *keyName, char *valueName) {
	KeyValueList *returnVal = new KeyValueList ();
	returnVal->appendkeyValuePair (keyName, valueName);
	free (keyName);
	free (valueName);
	return returnVal;
}

struct KeyValueList *pushBackKeyValue (struct KeyValueList *addToMe, char *keyName, char *valueName) {
	addToMe->appendkeyValuePair (keyName, valueName);
	free (keyName);
	free (valueName);
	return addToMe;
}

struct AtomicComputation *makeFilterWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, struct KeyValueList *useMe) {
	AtomicComputationPtr returnVal = std :: make_shared <ApplyFilter> (*input, *output, *projection, std :: string (nodeName), *useMe);
	returnVal->setShared (returnVal);
	delete output;
	delete input;
	delete projection;
	free (nodeName);
	return returnVal.get ();
}

struct AtomicComputation *makeApplyWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, char *opName, struct KeyValueList *useMe) {
	AtomicComputationPtr returnVal = std :: make_shared <ApplyLambda> (*input, *output, *projection, std :: string (nodeName), std :: string (opName), *useMe);
	returnVal->setShared (returnVal);
	delete output;
	delete input;
	delete projection;
	free (nodeName);
	free (opName);
	return returnVal.get ();
}

struct AtomicComputation *makeAggWithList (struct TupleSpec *output, struct TupleSpec *input, char *nodeName, struct KeyValueList *useMe) {
	AtomicComputationPtr returnVal = std :: make_shared <ApplyAgg> (*input, *output, *input, std :: string (nodeName), *useMe);
	returnVal->setShared (returnVal);
	delete output;
	delete input;
	free (nodeName);
	return returnVal.get ();
}

struct AtomicComputation *makeJoinWithList (struct TupleSpec *output, struct TupleSpec *lInput, struct TupleSpec *lProjection,
		struct TupleSpec *rInput, struct TupleSpec *rProjection, char *opName, struct KeyValueList *useMe) {
	AtomicComputationPtr returnVal = std :: make_shared <ApplyJoin> (*output, *lInput, *rInput, *lProjection,
		*rProjection, std :: string (opName), *useMe);
	returnVal->setShared (returnVal);
	free (opName);
	delete output;
	delete lInput;
	delete rInput;
	delete lProjection;
	delete rProjection;
	return returnVal.get ();
}

struct AtomicComputation *makeHashLeftWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, char *opName, struct KeyValueList *useMe) {
        AtomicComputationPtr returnVal = std :: make_shared <HashLeft> (*input, *output, *projection, std :: string (nodeName), std :: string (opName), *useMe);
        returnVal->setShared (returnVal);
        delete output;
        delete input;
        delete projection;
        free (nodeName);
        free (opName);
        return returnVal.get ();
}

struct AtomicComputation *makeHashRightWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, char *opName, struct KeyValueList *useMe) {
        AtomicComputationPtr returnVal = std :: make_shared <HashRight> (*input, *output, *projection, std :: string (nodeName), std :: string (opName), *useMe);
        returnVal->setShared (returnVal);
        delete output;
        delete input;
        delete projection;
        free (nodeName);
        free (opName);
        return returnVal.get ();
}



struct AtomicComputation *makeHashOneWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, struct KeyValueList *useMe) {
        AtomicComputationPtr returnVal = std :: make_shared <HashOne> (*input, *output, *projection, std :: string (nodeName), *useMe);
        returnVal->setShared (returnVal);
        delete output;
        delete input;
        delete projection;
        free (nodeName);
        return returnVal.get ();
}

struct AtomicComputation *makeFlattenWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, struct KeyValueList *useMe) {
        AtomicComputationPtr returnVal = std :: make_shared <Flatten> (*input, *output, *projection, std :: string (nodeName), *useMe);
        returnVal->setShared (returnVal);
        delete output;
        delete input;
        delete projection;
        free (nodeName);
        return returnVal.get ();
}



struct AtomicComputation *makeScanWithList (struct TupleSpec *output, char *dbName, char *setName, char *nodeName, struct KeyValueList *useMe) {
	AtomicComputationPtr returnVal = std :: make_shared <ScanSet> (*output, std :: string (dbName), std :: string (setName), std :: string (nodeName), *useMe);
	returnVal->setShared (returnVal);
	free (dbName);
	free (setName);
	free (nodeName);
	delete output;
	return returnVal.get ();
}

struct AtomicComputation *makeOutputWithList (struct TupleSpec *output, struct TupleSpec *input,
	char *dbName, char *setName, char *nodeName, struct KeyValueList *useMe) {
	AtomicComputationPtr returnVal = std :: make_shared <WriteSet> (*input, *output, *input,
		std :: string (dbName), std :: string (setName), std :: string (nodeName), *useMe);
	returnVal->setShared (returnVal);
	free (dbName);
	free (setName);
	free (nodeName);
	delete output;
	delete input;
	return returnVal.get ();
}


// Older functions that were here:
struct AttList* makeAttList(char* fromMe) {
    AttList* returnVal = new AttList();
    returnVal->appendAttribute(fromMe);
    free(fromMe);
    return returnVal;
}

struct AttList* pushBackAttribute(struct AttList* addToMe, char* fromMe) {
    addToMe->appendAttribute(fromMe);
    free(fromMe);
    return addToMe;
}

struct TupleSpec* makeTupleSpec(char* setName, struct AttList* useMe) {
    TupleSpec* returnVal = new TupleSpec(std::string(setName), *useMe);
    delete useMe;
    free(setName);
    return returnVal;
}

struct AtomicComputationList* makeAtomicComputationList(struct AtomicComputation* fromMe) {
    struct AtomicComputationList* returnVal = new AtomicComputationList();
    returnVal->addAtomicComputation(fromMe->getShared());
    return returnVal;
}

struct AtomicComputationList* pushBackAtomicComputation(struct AtomicComputationList* input,
                                                        struct AtomicComputation* addMe) {
    input->addAtomicComputation(addMe->getShared());
    return input;
}

struct TupleSpec* makeEmptyTupleSpec(char* setName) {
    TupleSpec* returnVal = new TupleSpec(std::string(setName));
    free(setName);
    return returnVal;
}

struct AtomicComputation* makeFilter(struct TupleSpec* output,
                                     struct TupleSpec* input,
                                     struct TupleSpec* projection,
                                     char* nodeName) {
    AtomicComputationPtr returnVal =
        std::make_shared<ApplyFilter>(*input, *output, *projection, std::string(nodeName));
    returnVal->setShared(returnVal);
    delete output;
    delete input;
    delete projection;
    free(nodeName);
    return returnVal.get();
}

struct AtomicComputation* makeApply(struct TupleSpec* output,
                                    struct TupleSpec* input,
                                    struct TupleSpec* projection,
                                    char* nodeName,
                                    char* opName) {
    AtomicComputationPtr returnVal = std::make_shared<ApplyLambda>(
        *input, *output, *projection, std::string(nodeName), std::string(opName));
    returnVal->setShared(returnVal);
    delete output;
    delete input;
    delete projection;
    free(nodeName);
    free(opName);
    return returnVal.get();
}

struct AtomicComputation* makeAgg(struct TupleSpec* output,
                                  struct TupleSpec* input,
                                  char* nodeName) {
    AtomicComputationPtr returnVal =
        std::make_shared<ApplyAgg>(*input, *output, *input, std::string(nodeName));
    returnVal->setShared(returnVal);
    delete output;
    delete input;
    free(nodeName);
    return returnVal.get();
}

struct AtomicComputation* makeJoin(struct TupleSpec* output,
                                   struct TupleSpec* lInput,
                                   struct TupleSpec* lProjection,
                                   struct TupleSpec* rInput,
                                   struct TupleSpec* rProjection,
                                   char* opName) {
    AtomicComputationPtr returnVal = std::make_shared<ApplyJoin>(
        *output, *lInput, *rInput, *lProjection, *rProjection, std::string(opName));
    returnVal->setShared(returnVal);
    free(opName);
    delete output;
    delete lInput;
    delete rInput;
    delete lProjection;
    delete rProjection;
    return returnVal.get();
}

struct AtomicComputation* makeHashLeft(struct TupleSpec* output,
                                       struct TupleSpec* input,
                                       struct TupleSpec* projection,
                                       char* nodeName,
                                       char* opName) {
    AtomicComputationPtr returnVal = std::make_shared<HashLeft>(
        *input, *output, *projection, std::string(nodeName), std::string(opName));
    returnVal->setShared(returnVal);
    delete output;
    delete input;
    delete projection;
    free(nodeName);
    free(opName);
    return returnVal.get();
}

struct AtomicComputation* makeHashRight(struct TupleSpec* output,
                                        struct TupleSpec* input,
                                        struct TupleSpec* projection,
                                        char* nodeName,
                                        char* opName) {
    AtomicComputationPtr returnVal = std::make_shared<HashRight>(
        *input, *output, *projection, std::string(nodeName), std::string(opName));
    returnVal->setShared(returnVal);
    delete output;
    delete input;
    delete projection;
    free(nodeName);
    free(opName);
    return returnVal.get();
}

struct AtomicComputation* makeHashOne(struct TupleSpec* output,
                                      struct TupleSpec* input,
                                      struct TupleSpec* projection,
                                      char* nodeName) {
    AtomicComputationPtr returnVal =
        std::make_shared<HashOne>(*input, *output, *projection, std::string(nodeName));
    returnVal->setShared(returnVal);
    delete output;
    delete input;
    delete projection;
    free(nodeName);
    return returnVal.get();
}

struct AtomicComputation* makeFlatten(struct TupleSpec* output,
                                      struct TupleSpec* input,
                                      struct TupleSpec* projection,
                                      char* nodeName) {
    AtomicComputationPtr returnVal =
        std::make_shared<Flatten>(*input, *output, *projection, std::string(nodeName));
    returnVal->setShared(returnVal);
    delete output;
    delete input;
    delete projection;
    free(nodeName);
    return returnVal.get();
}


struct AtomicComputation* makeScan(struct TupleSpec* output,
                                   char* dbName,
                                   char* setName,
                                   char* nodeName) {
    AtomicComputationPtr returnVal = std::make_shared<ScanSet>(
        *output, std::string(dbName), std::string(setName), std::string(nodeName));
    returnVal->setShared(returnVal);
    free(dbName);
    free(setName);
    free(nodeName);
    delete output;
    return returnVal.get();
}

struct AtomicComputation* makeOutput(struct TupleSpec* output,
                                     struct TupleSpec* input,
                                     char* dbName,
                                     char* setName,
                                     char* nodeName) {
    AtomicComputationPtr returnVal = std::make_shared<WriteSet>(
        *input, *output, *input, std::string(dbName), std::string(setName), std::string(nodeName));
    returnVal->setShared(returnVal);
    free(dbName);
    free(setName);
    free(nodeName);
    delete output;
    delete input;
    return returnVal.get();
}

// structure that stores a list of aliases from a FROM clause
}  // extern

#endif
