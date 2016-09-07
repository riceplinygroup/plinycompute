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

#ifndef TEST_15_CC
#define TEST_15_CC

#include "PDBServer.h"
#include "CatalogServer.h"
#include "StorageServer.h"
#include "CatalogClient.h"

int main () {

       std :: cout << "Starting up a catalog/storage server!!\n";
       pdb :: PDBLoggerPtr myLogger = make_shared <pdb :: PDBLogger> ("frontendLogFile.log");
       pdb :: PDBServer frontEnd (8108, 10, myLogger);
       frontEnd.addFunctionality <pdb :: CatalogServer> ("CatalogDir");
       frontEnd.addFunctionality <pdb :: CatalogClient> (8108, "localhost", myLogger);
       frontEnd.addFunctionality <pdb :: StorageServer> ("StorageDir", 1024 * 128, 128);

       frontEnd.startServer (nullptr);
}

#endif

