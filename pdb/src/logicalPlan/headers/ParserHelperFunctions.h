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

#ifndef IR_HEADERS_H
#define IR_HEADERS_H

#ifdef __cplusplus
extern "C" {
#endif

/******************************************************/
// VARIOUS STRUCTURES ASSOCIATED WITH PRODCUTION RULES
/******************************************************/

struct AttList;
struct AtomicComputation;
struct AtomicComputationList;
struct LogicalPlan;
struct AtomicComputationList;
struct TupleSpec;
struct KeyValueList;

/***************************************************************************/
// C FUNCTIONS TO MANIPULATE THE VARIOUS STRUCTURES PRODUCED BY THE PARSER
/***************************************************************************/
struct AtomicComputation *makeApplyWithList (struct TupleSpec *output,
                                             struct TupleSpec *input,
                                             struct TupleSpec *projection,
                                             char *nodeName,
                                             char *opName,
                                             struct KeyValueList *useMe);

struct KeyValueList *makeKeyValueList (char *keyName, char *valueName);
struct KeyValueList *pushBackKeyValue (struct KeyValueList *addToMe, char *keyName, char *valueName);
struct KeyValueList *makeEmptyKeyValueList ();
struct AtomicComputation *makeAggWithList (struct TupleSpec *output, struct TupleSpec *input, char *nodeName, struct KeyValueList *useMe);
struct AtomicComputation *makeFilterWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, struct KeyValueList *useMe);
struct AtomicComputation *makeJoinWithList (struct TupleSpec *output, struct TupleSpec *lInput, struct TupleSpec *lProjection,
                                            struct TupleSpec *rInput, struct TupleSpec *rProjection, char *opName, struct KeyValueList *useMe);
struct AtomicComputation *makeHashLeftWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, char *opName, struct KeyValueList *useMe);
struct AtomicComputation *makeHashRightWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, char *opName, struct KeyValueList *useMe);
struct AtomicComputation *makeHashOneWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, struct KeyValueList *useMe);
struct AtomicComputation *makeFlattenWithList (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *nodeName, struct KeyValueList *useMe);
struct AtomicComputation *makeScanWithList (struct TupleSpec *output, char *dbName, char *setName, char *nodeName, struct KeyValueList *useMe);
struct AtomicComputation *makeOutputWithList (struct TupleSpec *output, struct TupleSpec *input, char *dbName, char *setName, char *nodeName, struct KeyValueList *useMe);
struct AttList* makeAttList(char* fromMe);
struct TupleSpec* makeEmptyTupleSpec(char* setName);
struct AttList* pushBackAttribute(struct AttList* addToMe, char* fromMe);
struct AtomicComputationList* makeAtomicComputationList(struct AtomicComputation* fromMe);
struct TupleSpec* makeTupleSpec(char* setName, struct AttList* useMe);
struct AtomicComputationList* pushBackAtomicComputation(struct AtomicComputationList* input,
                                                        struct AtomicComputation* addMe);
struct LogicalPlan* makePlan(struct AtomicComputationList* computations);
struct AtomicComputation* makeOutput(
    struct TupleSpec* output, struct TupleSpec* input, char* dbName, char* setName, char* nodeName);
struct AtomicComputation* makeScan(struct TupleSpec* output,
                                   char* dbName,
                                   char* setName,
                                   char* nodeName);
// JiaNote: add one more parameter char* nodeName based on Chris' Join code
struct AtomicComputation* makeAgg(struct TupleSpec* output,
                                  struct TupleSpec* input,
                                  char* nodeName);
struct AtomicComputation* makeApply(struct TupleSpec* output,
                                    struct TupleSpec* input,
                                    struct TupleSpec* projection,
                                    char* nodeName,
                                    char* opName);
struct AtomicComputation* makeFilter(struct TupleSpec* output,
                                     struct TupleSpec* input,
                                     struct TupleSpec* projection,
                                     char* nodeName);

// JiaNote: remove one parameter char* nodeName based on Chris' Join code
struct AtomicComputation* makeJoin(struct TupleSpec* output,
                                   struct TupleSpec* lInput,
                                   struct TupleSpec* lProjection,
                                   struct TupleSpec* rInput,
                                   struct TupleSpec* rProjection,
                                   char* opName);


struct AtomicComputation* makeHashRight(struct TupleSpec* output,
                                        struct TupleSpec* input,
                                        struct TupleSpec* projection,
                                        char* nodeName,
                                        char* opName);
struct AtomicComputation* makeHashLeft(struct TupleSpec* output,
                                       struct TupleSpec* input,
                                       struct TupleSpec* projection,
                                       char* nodeName,
                                       char* opName);
struct AtomicComputation* makeHashOne(struct TupleSpec* output,
                                      struct TupleSpec* input,
                                      struct TupleSpec* projection,
                                      char* nodeName);
struct AtomicComputation* makeFlatten(struct TupleSpec* output,
                                      struct TupleSpec* input,
                                      struct TupleSpec* projection,
                                      char* nodeName);
#ifdef __cplusplus
}
#endif

#endif
