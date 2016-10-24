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

%{
	#include "Lexer.h"
	#include "ParserHelperFunctions.h" 
	#include <stdio.h>
	#include <stdlib.h>
	#include <string.h>

%}

// this stores all of the types returned by production rules
%union {
	char *myChar;
	struct LogicalPlan *myPlan;
	struct Output *myOutput;
	struct OutputList *myOutputList;
	struct InputList *myInputList;
	struct Input *myInput;
	struct ComputationList *myComputationList;
	struct Computation *myComputation;
	struct TupleSpec *myTupleSpec;
	struct AttList *myAttList;
};

%pure-parser
%lex-param {void *scanner}
%parse-param {void *scanner}
%parse-param {struct LogicalPlan **myPlan}

%token <myChar> STRING 
%token <myChar> IDENTIFIER
%token FILTER
%token APPLY
%token GETS
%token INPUTS
%token OUTPUTS
%token COMPUTATIONS

%type <myPlan> LogicalQueryPlan
%type <myOutput> Output
%type <myOutputList> OutputList
%type <myInputList> InputList
%type <myInput> Input
%type <myComputationList> ComputationList
%type <myComputation> Computation
%type <myTupleSpec> TupleSpec
%type <myAttList> AttList

%start LogicalQueryPlan

//******************************************************************************
// SECTION 3
//******************************************************************************
/* This is the PRODUCTION RULES section which defines how to "understand" the 
 * input language and what action to take for each "statment"
 */

%%

LogicalQueryPlan : OUTPUTS ':' OutputList INPUTS ':' InputList COMPUTATIONS ':' ComputationList
{
	$$ = makePlan ($3, $6, $9);
	*myPlan = $$;
}
; 

/********************************/
/** THIS IS A LIST OF OUTPUTS ***/
/********************************/

OutputList : OutputList ',' Output
{
	$$ = pushBackOutput ($1, $3);
}

| Output
{
	$$ = makeOutputList ($1);
}
;

Output : '(' STRING ',' STRING ')' GETS TupleSpec
{
	$$ = makeOutput ($7, $2, $4);
}
;

/*******************************/
/** THIS IS A LIST OF INPUTS ***/
/*******************************/

InputList : InputList ',' Input
{
	$$ = pushBackInput ($1, $3);
}

| Input
{
	$$ = makeInputList ($1);
}
;

Input : TupleSpec GETS '(' STRING ',' STRING ')' 
{
	$$ = makeInput ($1, $4, $6);
}
;

/*************************************/
/** THIS IS A LIST OF COMPUTATIONS ***/
/*************************************/

ComputationList : ComputationList  Computation
{
	$$ = pushBackComputation ($1, $2);
}

| Computation 
{
	$$ =  makeComputationList ($1);
}
;

Computation: TupleSpec GETS APPLY '(' TupleSpec ',' TupleSpec ',' STRING ')'
{
	$$ = makeApply ($1, $5, $7, $9);
}

| TupleSpec GETS FILTER '(' TupleSpec ',' TupleSpec ')'
{
	$$ = makeFilter ($1, $5, $7);
}
;

/*********************************************/
/** THIS IS A TUPLE SPEC, LIKE C (a, b, c) ***/
/*********************************************/

TupleSpec : IDENTIFIER '(' AttList ')'
{
	$$ = makeTupleSpec ($1, $3);
}
;

AttList : AttList ',' IDENTIFIER
{
	$$ = pushBackAttribute ($1, $3);
}

| IDENTIFIER
{
	$$ = makeAttList ($1);
}
;


%%

