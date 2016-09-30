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
#include "CheckEmployees.h"
#include "MyDB_BufferManager.h"

using namespace pdb;

int main () {

        MyDB_BufferManager myManager (1024 * 1024, 128, "tmpFile");
        MyDB_TablePtr dataTable = std :: make_shared <MyDB_Table> ("tmpTable", "tmpDataFile");
        MyDB_TablePtr resultTable = std :: make_shared <MyDB_Table> ("resultTable", "resultDataFile");

        // create some pages, loading them with random data
        for (int iter = 0; iter < 10; iter++) {

                // get a page
                MyDB_PageHandle myPage = myManager.getPage (dataTable, iter);
                makeObjectAllocatorBlock (myPage->getBytes (), 1024 * 1024, true);

                // write a bunch of supervisors to it
                Handle <Vector <Handle <Supervisor>>> supers = makeObject <Vector <Handle <Supervisor>>> ();
                try {
                        for (int i = 0; true; i++) {

                                Handle <Supervisor> super = makeObject <Supervisor> ("Joe Johnson", 20 + (i % 29));
                                supers->push_back (super);
                                for (int j = 0; j < 10; j++) {
                                        Handle <Employee> temp;
                                        if (j % 2 == 0)
                                                temp = makeObject <Employee> ("Steve Stevens", 20 + ((i + j) % 29));
                                        else
                                                temp = makeObject <Employee> ("Albert Albertson", 20 + ((i + j) % 29));
                                        (*supers)[i]->addEmp (temp);
                                }
                        }

                // an exception means that we filled the page with data
                       } catch (NotEnoughSpace &e) {
                        getRecord (supers);
                        myPage->wroteBytes ();
                }
        }

        // now, we process those pages of data, to answer a query
        makeObjectAllocatorBlock (1024 * 1024, true);
        Handle <CheckEmployee> myQuery = makeObject <CheckEmployee> (std :: string ("Steve Stevens"));

        // get a query processor and intitialize it
        auto queryProc = myQuery->getProcessor ();
        queryProc->initialize ();
        int posInOutTable = 0;

        auto filterProc = myQuery->getFilterProcessor();
        filterProc->initialize();
        auto projProc = myQuery->getProjectionProcessor();
        projProc->initialize();
        // get the first output page and load it into the query processor
        //MyDB_PageHandle myOutPage = myManager.getPage (resultTable, posInOutTable);
        //queryProc->loadOutputPage (myOutPage->getBytes (), 1024 * 1024);

        void *tempPage = malloc(1024 * 256);
        filterProc->loadOutputPage(tempPage, 1024*256);


        // loop through the input pages
        for (int iter = 0; iter <= 10; iter++) {

                // get the first input page and load it into the query processor
                if (iter < 10) {
                        MyDB_PageHandle myInPage = myManager.getPage (dataTable, iter);
                        filterProc->loadInputPage (myInPage->getBytes ());
                } else {
                        filterProc->finalize ();
                }
std::cout << "Filling filtering page" << std::endl;
                // while we keep producing results, write the output pages
                while (filterProc->fillNextOutputPage ()) {

                        MyDB_PageHandle myOutPage = myManager.getPage (resultTable, posInOutTable);
                        
                        projProc->initialize();
                        projProc->loadOutputPage (myOutPage->getBytes (), 1024 * 1024);
                        projProc->loadInputPage(tempPage);
                        while(projProc->fillNextOutputPage()) {
                                std::cout << "Inside projection" << std::endl;
                                filterProc->clearOutputPage();
                                myOutPage->wroteBytes ();
                                myOutPage = myManager.getPage (resultTable, ++posInOutTable);
                                projProc->loadOutputPage (myOutPage->getBytes (), 1024 * 1024);
                        }
                        projProc->finalize();
                        projProc->fillNextOutputPage();
                        projProc->clearInputPage(); 
                        filterProc->clearOutputPage();
                        free(tempPage);
                        tempPage = malloc(1024*256);
                        filterProc->loadOutputPage(tempPage, 1024*256);
                        // tell the buffer manager that we wrote the current output page
                        //myOutPage->wroteBytes ();

                        // and get the next output page
                        //myOutPage = myManager.getPage (resultTable, ++posInOutTable);
                        //queryProc->loadOutputPage (myOutPage->getBytes (), 1024 * 1024);
                }
        }

        // finally, print out all of the results
        /*
        for (int pageNo = 0; pageNo < posInOutTable; pageNo++) {
                MyDB_PageHandle writtenPage = myManager.getPage (resultTable, pageNo);
                auto *temp = (Record <Vector <Handle <Vector <Handle <Employee>>>>> *) writtenPage->getBytes ();
                auto myGuys = temp->getRootObject ();
                for (int i = 0; i < myGuys->size (); i++) {
                        for (int j = 0; j < (*myGuys)[i]->size (); j++) {
                                (*((*myGuys)[i]))[j]->print ();
                                std :: cout << "\n";
                        }
                }
        }
        */
}
