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
	#include "ParserHelperFunctions.h" 
	#include <stdio.h>
	#include <stdlib.h>
	#include <string.h>

	#if YYBISON
		union YYSTYPE;
		int yylex(union YYSTYPE *, void *);
	#endif

	#define YYDEBUG 1

	typedef void* yyscan_t;
	void yyerror(yyscan_t scanner, struct AtomicComputationList **myStatement, const char *);
%}

// this stores all of the types returned by production rules
%union {
	char *myChar;
	struct AtomicComputationList *myAtomicComputationList;
	struct AtomicComputation *myAtomicComputation;
	struct TupleSpec *myTupleSpec;
	struct AttList *myAttList;
	struct KeyValueList *myKeyValueList;
};

%pure-parser
%lex-param {void *scanner}
%parse-param {void *scanner}
%parse-param {struct AtomicComputationList **myPlan}

%token FILTER 
%token APPLY
%token SCAN
%token AGG 
%token JOIN 
%token OUTPUT 
%token GETS
%token HASHLEFT
%token HASHRIGHT
%token HASHONE
%token FLATTEN
%token <myChar> IDENTIFIER
%token <myChar> STRING 

%type <myAtomicComputationList> LogicalQueryPlan
%type <myAtomicComputationList> AtomicComputationList
%type <myAtomicComputation> AtomicComputation
%type <myTupleSpec> TupleSpec
%type <myAttList> AttList
%type <myKeyValueList> ListKeyValuePairs

%start LogicalQueryPlan

//******************************************************************************
// SECTION 3
//******************************************************************************
/* This is the PRODUCTION RULES section which defines how to "understand" the 
 * input language and what action to take for each "statment"
 */

%%

LogicalQueryPlan : AtomicComputationList
{
	$$ = $1;
	*myPlan = $$;
}
; 

/*************************************/
/** THIS IS A LIST OF COMPUTATIONS ***/
/*************************************/

AtomicComputationList : AtomicComputationList AtomicComputation
{
	$$ = pushBackAtomicComputation ($1, $2);
}

| AtomicComputation 
{
	$$ =  makeAtomicComputationList ($1);
}
;

/***************************************/
/** THIS IS A PARTICULAR COMPUTATION ***/
/***************************************/



// Atomic Computation: Apply:
AtomicComputation: TupleSpec GETS APPLY '(' TupleSpec ',' TupleSpec ',' STRING ',' STRING ')'
{
	$$ = makeApply ($1, $5, $7, $9, $11);
}

// ss107: Update to older TCAP:
| TupleSpec GETS APPLY '(' TupleSpec ',' TupleSpec ',' STRING ',' STRING ',' '[' ListKeyValuePairs ']' ')'
{
	$$ = makeApplyWithList ($1, $5, $7, $9, $11, $14);
}



// Atomic Computation: Aggregate:
| TupleSpec GETS AGG '(' TupleSpec ',' STRING ')'
{
	$$ = makeAgg ($1, $5, $7);
}

// ss107: Update to older TCAP:
| TupleSpec GETS AGG '(' TupleSpec ',' STRING ',' '[' ListKeyValuePairs ']' ')'
{
	$$ = makeAggWithList ($1, $5, $7, $10);
}



// Atomic Computation: Scan:
| TupleSpec GETS SCAN '(' STRING ',' STRING ',' STRING ')'
{
	$$ = makeScan ($1, $5, $7, $9);
}

// ss107: Update to older TCAP:
| TupleSpec GETS SCAN '(' STRING ',' STRING ',' STRING ',' '[' ListKeyValuePairs ']' ')'
{
	$$ = makeScanWithList ($1, $5, $7, $9, $12);
}



// Atomic Computation: Output:
| TupleSpec GETS OUTPUT '(' TupleSpec ',' STRING ',' STRING ',' STRING ')'
{
	$$ = makeOutput ($1, $5, $7, $9, $11);
}

// ss107: Update to older TCAP:
| TupleSpec GETS OUTPUT '(' TupleSpec ',' STRING ',' STRING ',' STRING ',' '[' ListKeyValuePairs ']' ')'
{
	$$ = makeOutputWithList ($1, $5, $7, $9, $11, $14);
}



// Atomic Computation: Join:
| TupleSpec GETS JOIN '(' TupleSpec ',' TupleSpec ',' TupleSpec ',' TupleSpec ',' STRING ')'
{
	$$ = makeJoin ($1, $5, $7, $9, $11, $13);
}

// ss107: Update to older TCAP:
| TupleSpec GETS JOIN '(' TupleSpec ',' TupleSpec ',' TupleSpec ',' TupleSpec ',' STRING ',' '[' ListKeyValuePairs ']' ')'
{
	$$ = makeJoinWithList ($1, $5, $7, $9, $11, $13, $16);
}



// Atomic Computation: Filter:
| TupleSpec GETS FILTER '(' TupleSpec ',' TupleSpec ',' STRING ')'
{
	$$ = makeFilter ($1, $5, $7, $9);
}

// ss107: Update to older TCAP:
| TupleSpec GETS FILTER '(' TupleSpec ',' TupleSpec ',' STRING ',' '[' ListKeyValuePairs ']' ')'
{
	$$ = makeFilterWithList ($1, $5, $7, $9, $12);
}


// Atomic Computation: Hashleft:
| TupleSpec GETS HASHLEFT '(' TupleSpec ',' TupleSpec ',' STRING ',' STRING ')'
{
	$$ = makeHashLeft ($1, $5, $7, $9, $11);
}

// ss107: Update to older TCAP:
| TupleSpec GETS HASHLEFT '(' TupleSpec ',' TupleSpec ',' STRING ',' STRING ',' '[' ListKeyValuePairs ']' ')'
{
	$$ = makeHashLeftWithList ($1, $5, $7, $9, $11, $14);
}




// Atomic Computation: Hashright:
| TupleSpec GETS HASHRIGHT '(' TupleSpec ',' TupleSpec ',' STRING ',' STRING ')'
{
	$$ = makeHashRight ($1, $5, $7, $9, $11);
}

// ss107: Update to older TCAP:
| TupleSpec GETS HASHRIGHT '(' TupleSpec ',' TupleSpec ',' STRING ',' STRING ',' '[' ListKeyValuePairs ']' ')'
{
	$$ = makeHashRightWithList ($1, $5, $7, $9, $11, $14);
}

// Atomic Computation: Hashone:
| TupleSpec GETS HASHONE '(' TupleSpec ',' TupleSpec ',' STRING ')'
{
        $$ = makeHashOne ($1, $5, $7, $9);
}

// ss107: Update to older TCAP:
| TupleSpec GETS HASHONE '(' TupleSpec ',' TupleSpec ',' STRING ',' '[' ListKeyValuePairs ']' ')'
{
        $$ = makeHashOneWithList ($1, $5, $7, $9, $12);
}



// Atomic Computation: Flatten:
| TupleSpec GETS FLATTEN '(' TupleSpec ',' TupleSpec ',' STRING ')'
{
        $$ = makeFlatten ($1, $5, $7, $9);
}

// ss107: Update to older TCAP:
| TupleSpec GETS FLATTEN '(' TupleSpec ',' TupleSpec ',' STRING ',' '[' ListKeyValuePairs ']' ')'
{
        $$ = makeFlattenWithList ($1, $5, $7, $9, $12);
}
;

/*********************************************/
/** THIS IS A TUPLE SPEC, LIKE C (a, b, c) ***/
/*********************************************/

TupleSpec : IDENTIFIER '(' AttList ')'
{
	$$ = makeTupleSpec ($1, $3);
}

| IDENTIFIER '(' ')'
{
	$$ = makeEmptyTupleSpec ($1);
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


/*********************************************/
/** THIS IS A LIST OF (KEY, VALUE) PAIRS ***/
/*********************************************/

ListKeyValuePairs : ListKeyValuePairs ',' '(' STRING ',' STRING ')' 
{
	$$ = pushBackKeyValue($1, $4, $6);
}

| '(' STRING ',' STRING ')'
{
	$$ = makeKeyValueList($2, $4);
}

| 	
{
	$$ = makeEmptyKeyValueList();
}
;



%%

int yylex(YYSTYPE *, void *);

