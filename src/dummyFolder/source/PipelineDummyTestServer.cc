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

// Added to avoid linker errors
#ifndef DUMMY_SERVER_CC
#define DUMMY_SERVER_CC
/*
#include <cstddef>
#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

#include "PipelineDummyTestServer.h"
#include "SimpleRequestHandler.h"
#include "BuiltInObjectTypeIDs.h"
#include "SimpleRequestResult.h"
#include "PangeaStorageServer.h"
#include "QueriesAndPlan.h"
#include "NewExecutionPipeline.h"
#include "LogicalPlanBuilder.h"

//TODO remove:
#include "Employee.h"

namespace pdb {

PipelineDummyTestServer :: PipelineDummyTestServer () {
}


PipelineDummyTestServer :: ~PipelineDummyTestServer () {}

void PipelineDummyTestServer :: registerHandlers (PDBServer &forMe) {

        forMe.registerHandler (QueriesAndPlan_TYPEID, make_shared <SimpleRequestHandler
<QueriesAndPlan>> (
                [&] (Handle <QueriesAndPlan> request, PDBCommunicatorPtr sendUsingMe) {

                    std :: cout << "Frontend got a request for QueriesAndPlan" << std :: endl;
                    std :: cout << request->getPlan() << std :: endl;

                    shared_ptr<SafeResult<LogicalPlan>> result =
buildLogicalPlan(request->getPlan());

                    Handle<Vector <Handle <BaseQuery>>> queries = request->getQueries();

                    result->apply(
                        [&](LogicalPlan final)
                        {
                            {

                                NewExecutionPipeline myPipe (
                                        [] () -> std :: pair <void *, size_t> {
                                            std :: cout << "Asking for a new page.\n";
                                            void *myPage = malloc (4 * 64 * 1024);
                                            std :: cout << "Page was " << (size_t) myPage <<
"!!!\n";
                                            return std :: make_pair (myPage, 4 * 64 * 1024);
                                        },
                                        [] (void *page, size_t pageSize) {
                                            std :: cout << "Writing back page of size " << pageSize
<< "!!!\n";
                                            std :: cout << "Page was " << (size_t) page << "!!!\n";
                                            Handle <Vector <Handle <Employee>>> temp = ((Record
<Vector <Handle <Employee>>> *) page)->getRootObject ();
                                            std :: cout << "Found " << temp->size () << "
objects.\n";
                                            for (int i = 0; i < temp->size (); i++) {
                                                (*temp)[i]->print ();
                                                std :: cout << " ";
                                            }
                                            std :: cout << "\n";
                                            free (page);
                                        },
                                        [] (void *page, size_t pageSize) {
                                            std :: cout << "Freeing page of size " << pageSize <<
"!!!\n";
                                            free (page);
                                        }, this);

                                myPipe.build(queries, final);

                                // run the pipeline!!!
                                // the "1" indicates that we are writing out the column in position
1 (the second column) from the output tuples
                                myPipe.run (1);
                            }
                        },
                        [&](const string &errorMsg)
                        {
                            return -1;
                        });


                    return std :: make_pair (true, std :: string("execution complete"));

        } ));

}


}
*/
#endif
