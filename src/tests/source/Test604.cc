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
#ifndef TEST_604_CC
#define TEST_604_CC

#include <memory>

#include "Handle.h"
#include "Lambda.h"
#include "Supervisor.h"
#include "Employee.h"
#include "FilterQueryExecutor.h"
#include "LogicalPlanBuilder.h"
#include "LambdaCreationFunctions.h"
#include "Lexer.h"
#include "Parser.h"
#include "ExecutionPipeline.h"
#include "TupleSetIterator.h"
#include "PangeaStorageServer.h"
#include "UseTemporaryAllocationBlock.h"


using std::shared_ptr;

using namespace pdb;

class SillyQuery : public BaseQuery {

public:

	Lambda <bool> getSelection (Handle <Supervisor> &checkMe) {
		return makeLambdaFromMethod (checkMe, getSteve) == makeLambdaFromMember (checkMe, me);
	}

	Lambda <Handle <Employee>> getProjection (Handle <Supervisor> &checkMe) {
		return makeLambdaFromMethod (checkMe, getMe);
	}

	virtual void toMap(std :: map <std :: string, GenericLambdaObjectPtr> &fillMe, int &identifier) override {
		Handle <Supervisor> temp = nullptr;
    	getSelection(temp).toMap (fillMe, identifier);
    	getProjection(temp).toMap (fillMe, identifier);
    }
};

int main (int argc, char * argv[]) {
	

	string program = "A(a) = load \"myDB mySet\"\n"
			"@exec \"attAccess_2\"\n"
			"B(a,b) = hoist \"someAtt\" from A[a] retain all\n"
			"@exec \"methodCall_1\"\n"
			"C(a,b,c) = apply method \"someMethod\" to B[a] retain all\n"
			"@exec \"==_0\"\n"
			"D(a,b) = apply func \"someFunc\" to C[c,b] retain a\n"
			"E(a) = filter D by b retain a\n"
			"@exec \"methodCall_3\"\n"
			"F(a,b) = apply method \"someMethod\" to E[a] retain a\n"
			"store F[b] \"myDB myOutput\"";

	shared_ptr<SafeResult<LogicalPlan>> result = buildLogicalPlan(program);


    int numPagesToWrite;
    if (argc == 1) {
        numPagesToWrite = 1;
        std :: cout << "to generate 1 pages by default..." << std :: endl;
    } else {
        numPagesToWrite = atoi(argv[1]);
        std :: cout << "to generate "<< numPagesToWrite << " by default..." << std :: endl;
    }

    ConfigurationPtr conf = make_shared < Configuration > ();
    pdb :: PDBLoggerPtr logger = make_shared < pdb :: PDBLogger> (conf->getLogFile());
    SharedMemPtr shm = make_shared< SharedMem > (conf->getShmSize(), logger);
    pdb :: PDBWorkerQueuePtr workers = make_shared < pdb :: PDBWorkerQueue > (logger, conf->getMaxConnections()); 
    pdb :: PangeaStorageServerPtr storage = make_shared<pdb :: PangeaStorageServer> (shm, workers, logger, conf);	
    storage->startFlushConsumerThreads();

    //add database
    storage->addDatabase ("myDB");
    storage->addSet("myDB", "mySet");
    storage->addSet("myDB", "myOutput");
    SetPtr inputSet = storage->getSet(std :: pair <std :: string, std :: string> ("myDB", "mySet"));
    storage->getCache()->pin(inputSet, MRU, Write);

	//writing data to the set        
    int pagesWritten = 0;

    while (pagesWritten < numPagesToWrite) {
        PDBPagePtr page = storage->getNewPage(std :: pair <std :: string, std :: string>("myDB", "mySet"));
        if (page == nullptr) {
            std :: cout << "can't get page, exit..." << std :: endl;
            exit (EXIT_FAILURE);
        }
        
        const pdb :: UseTemporaryAllocationBlock block{page->getBytes(), page->getSize()};
        pdb :: Handle <pdb :: Vector <pdb :: Handle <pdb :: Supervisor>>> supers = pdb :: makeObject <pdb :: Vector <pdb :: Handle<pdb :: Supervisor>>> ();

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

        } catch ( pdb :: NotEnoughSpace &n ) {
            //now we can unpin this page
            cout << "we have finished one page!" << std :: endl;
            getRecord (supers);                 
            page->unpin();
            pagesWritten ++;
        }            
    }

    //let's flush
    {
        int num = storage->getCache()->unpinAndEvictAllDirtyPages();
        std :: cout << num << " pages are added to flush buffer!" << std :: endl;
        std :: cout << "sleep 5 seconds to wait for flushing threads to be scheduled..." << std :: endl;
        sleep (5);
        std :: cout << "done flushing!" << std :: endl;
	}


	result->apply(
			[&](LogicalPlan final)
			{
				{

					ExecutionPipeline myPipe (
							[] () -> std :: pair <void *, size_t> {
								std :: cout << "Asking for a new page.\n";
								void *myPage = malloc (4 * 64 * 1024);
								std :: cout << "Page was " << (size_t) myPage << "!!!\n";
								return std :: make_pair (myPage, 4 * 64 * 1024);
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
								//free (page);
							}, storage);

					shared_ptr<BaseQuery> queryToExecute = std::make_shared<SillyQuery>();
					myPipe.build(queryToExecute, final);

					// run the pipeline!!!
					// the "1" indicates that we are writing out the column in position 1 (the second column) from the output tuples
					myPipe.run (1);
				}
			},
			[&](const string &errorMsg)
			{
				return -1;
			});
}


#endif
