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

#include "Handle.h"
#include "Lambda.h"
#include "Supervisor.h"
#include "Employee.h"
#include "FilterQueryExecutor.h"
#include "LambdaCreationFunctions.h"
#include "Lexer.h"
#include "Parser.h"
#include "Pipeline.h"
#include "TupleSetIterator.h"

using namespace pdb;

class SillyQuery {

public:

	Lambda <bool> getSelection (Handle <Supervisor> &checkMe) {
		return makeLambdaFromMethod (checkMe, getSteve) == makeLambdaFromMember (checkMe, me);
	}

	Lambda <Handle <Employee>> getProjection (Handle <Supervisor> &checkMe) {
		return makeLambdaFromMethod (checkMe, getMe);
	}
};

int main () {
	
	// the query to process
	SillyQuery temp;

	// get the various lambdas
	std :: map <std :: string, GenericLambdaObjectPtr> fillMe;
	int identifier = 0;
	Handle <Supervisor> checkMe = nullptr;
	
	// get the lambdas
	Lambda <bool> selectionLambda = temp.getSelection (checkMe);
	Lambda <Handle <Employee>> projectionLambda = temp.getProjection (checkMe);

	// build up a map of the lamdas
	selectionLambda.toMap (fillMe, identifier);
	projectionLambda.toMap (fillMe, identifier);

	std :: cout << "Are " << fillMe.size () << " items in the map.\n";
	for (auto &a : fillMe) {
		std :: cout << a.first << "\n";
	}

	// here is a hand compilation of the plan for the above selection
        std :: string myLogicalPlan =

                "Outputs:                                       \n      \
                                                                \n      \
                 (\"myDB\", \"mySet\") <= F(b)                  \n      \
                                                                \n      \
                Inputs:                                         \n      \
                                                                \n      \
                A(a) <= (\"myDB\", \"mySet\")                   \n      \
                                                                \n      \
                Computations:                                   \n      \
                                                                \n      \
		F (a, b) <= Apply (E (a), E (a), \"methodCall_3\")   	\n      \
                E (a) <=  Filter (D(b), D(a))                   	\n      \
                D (a, b) <= Apply (C(c, b), C(a), \"==_0\")  		\n      \
                C(a, b, c) <= Apply (B(a), B(a, b), \"methodCall_1\")  	\n      \
                B(a, b) <= Apply (A(a), A(a), \"attAccess_2\")";


	// parse the logical plan
        myLogicalPlan.push_back ('\0');
        yyscan_t scanner;
        LexerExtra extra { "" };
        yylex_init_extra (&extra, &scanner);
        const YY_BUFFER_STATE buffer { yy_scan_string (myLogicalPlan.data(), scanner) };
        LogicalPlan *final = nullptr;
        const int parseFailed { yyparse (scanner, &final) };
        yy_delete_buffer (buffer, scanner);
        yylex_destroy (scanner);

        if (parseFailed) {
                std :: cout << "Parse error: " << extra.errorMessage;
        }

	// create a, loading with random data
	void *myPage = malloc (1024 * 1024);
       	makeObjectAllocatorBlock (myPage, 1024 * 1024, true);

	// write a bunch of supervisors to it
       	Handle <Vector <Handle <Supervisor>>> supers = makeObject <Vector <Handle <Supervisor>>> ();
       	try {
               	for (int i = 0; true; i++) {

                       	Handle <Supervisor> super = makeObject <Supervisor> ("Steve Stevens", 20 + (i % 29));
                       	supers->push_back (super);
                       	for (int j = 0; j < 10; j++) {
                               	Handle <Employee> temp;
				if (i % 2 == 0)
					temp = makeObject <Employee> ("Steve Stevens", 20 + ((i + j) % 29));
				else
					temp = makeObject <Employee> ("Albert Albertson", 20 + ((i + j) % 29));
                               	(*supers)[i]->addEmp (temp);
                       	}
               	}

	// an exception means that we filled the page with data
       	} catch (NotEnoughSpace &e) {}

	{
		Handle <Vector <Handle <Object>>> tempVec = unsafeCast <Vector <Handle <Object>>> (supers);
		TupleSetIterator myIterator (tempVec, 128);
		Pipeline myPipe (
			[] () -> std :: pair <void *, size_t> {
				std :: cout << "Asking for a new page.\n";
				void *myPage = malloc (64 * 1024);
				std :: cout << "Page was " << (size_t) myPage << "!!!\n";
				return std :: make_pair (myPage, 64 * 1024);
			},
			[] (void *page, size_t pageSize) {
				std :: cout << "Writing back page of size " << pageSize << "!!!\n";
				std :: cout << "Page was " << (size_t) page << "!!!\n";
				Handle <Vector <Handle <Employee>>> temp = ((Record <Vector <Handle <Employee>>> *) page)->getRootObject ();
				std :: cout << "Found " << temp->size () << " objects.\n";
				for (int i = 0; i < temp->size (); i++) {
					(*temp)[i]->print ();
					std :: cout << " ";
				}
				std :: cout << "\n";
				free (page);
			}, 
			[] (void *page, size_t pageSize) {
				std :: cout << "Freeing page of size " << pageSize << "!!!\n";
				free (page);
			},
			myIterator);
	
		// create the pipeline
		Input &myInput = final->getInputs ().getProducer ("A");
		ComputationPtr firstOp = final->getComputations ().getProducingComputation ("B");
		myPipe.addStage (fillMe ["attAccess_2"]->getExecutor (myInput.getOutput (), firstOp->getInput (), firstOp->getProjection ()));
	
		ComputationPtr secOp = final->getComputations ().getProducingComputation ("C");
		myPipe.addStage (fillMe ["methodCall_1"]->getExecutor (firstOp->getOutput (), secOp->getInput (), secOp->getProjection ()));
	
		ComputationPtr thirdOp = final->getComputations ().getProducingComputation ("D");
		myPipe.addStage (fillMe ["==_0"]->getExecutor (secOp->getOutput (), thirdOp->getInput (), thirdOp->getProjection ()));
	
		ComputationPtr fourthOp = final->getComputations ().getProducingComputation ("E");
		myPipe.addStage (std :: make_shared <FilterQueryExecutor> (thirdOp->getOutput (), fourthOp->getInput (), fourthOp->getProjection ()));
	
		ComputationPtr fifthOp = final->getComputations ().getProducingComputation ("F");
		myPipe.addStage (fillMe ["methodCall_3"]->getExecutor (fourthOp->getOutput (), fifthOp->getInput (), fifthOp->getProjection ()));
	
		// kill all of the pointers, so the only links to the pipeline stages are in the pipeline itself
		firstOp = secOp = thirdOp = fourthOp = fifthOp = nullptr;

		// run the pipeline!!!
		// the "1" indicates that we are writing out the column in position 1 (the second column) from the output tuples
		myPipe.run (1);
	}	
	
	// clean everything up!!
	supers = nullptr;
	free (myPage);
	delete final;
}
