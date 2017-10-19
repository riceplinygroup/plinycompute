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
#include <iostream>
#include "PDBServer.h"

using namespace pdb;

void logToFile(const pdb::PDBLoggerPtr &logger) {
    logger->fatal("This is a FATAL");
    logger->error("This is a ERROR");
    logger->info("This is a INFO");
    logger->debug("This is a DEBUG");
    logger->trace("This is a TRACE");
}


int main() {


    std::string backendLoggerFile = "myLogFile.log";
    pdb::PDBLoggerPtr logger = make_shared<pdb::PDBLogger>(backendLoggerFile);

    logger->fatal("This is a SPARATOR ======   Log Level FATAL ===================  ");
    logger->setLoglevel(FATAL);
    logToFile(logger);


    logger->fatal("This is a SPARATOR ======   Log Level ERROR ===================  ");
    logger->setLoglevel(ERROR);
    logToFile(logger);


    logger->fatal("This is a SPARATOR ======   Log Level INFO ===================  ");
    logger->setLoglevel(INFO);
    logToFile(logger);


    logger->fatal("This is a SPARATOR ======   Log Level DEBUG ===================  ");
    logger->setLoglevel(DEBUG);
    logToFile(logger);


    logger->fatal("This is a SPARATOR ======   Log Level TRACE ===================  ");
    logger->setLoglevel(TRACE);
    logToFile(logger);

    return 0;
}
