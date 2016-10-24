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
struct Computation;
struct ComputationList;
struct Input;
struct InputList;
struct OutputList;
struct LogicalPlan;
struct ComputationList;
struct TupleSpec;
struct Output;

/******************************************************/
// C FUNCTIONS TO MANIPULATE THE VARIOUS STRUCTURES
/******************************************************/

struct AttList *makeAttList (char *fromMe);
struct Output *makeOutput (struct TupleSpec *fromMe, char *dbName, char *setName);
struct AttList *pushBackAttribute (struct AttList *addToMe, char *fromMe);
struct Computation *makeFilter (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection);
struct Computation *makeApply (struct TupleSpec *output, struct TupleSpec *input, struct TupleSpec *projection, char *name);
struct ComputationList *makeComputationList (struct Computation *fromMe);
struct TupleSpec *makeTupleSpec (char *setName, struct AttList *useMe);
struct ComputationList *pushBackComputation (struct ComputationList *input, struct Computation *addMe);
struct Input *makeInput (struct TupleSpec *output, char *dbName, char *setName);
struct InputList *makeInputList (struct Input *fromMe);
struct InputList *pushBackInput (struct InputList *intoMe, struct Input *pushMe);
struct OutputList *makeOutputList (struct Output *fromMe);
struct OutputList *pushBackOutput (struct OutputList *intoMe, struct Output *pushMe);
struct LogicalPlan *makePlan (struct OutputList *outputs, struct InputList *inputs, struct ComputationList *computations);

#ifdef __cplusplus
}
#endif

#endif
