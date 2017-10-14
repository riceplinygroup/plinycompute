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

#ifndef TEST_50_H
#define TEST_50_H

#include "PDBString.h"
#include "DistributedStorageManagerClient.h"
#include "Supervisor.h"
#include "Employee.h"
#include "DataTypes.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

/* This test uses data and selection of builtInType to demonstrate a distributed query with
 * distributed storage */


using namespace pdb;
int main(int argc, char* argv[]) {

    pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");
    // Step 1. Create Database and Set
    pdb::DistributedStorageManagerClient temp(8108, "localhost", clientLogger);

    string errMsg;

    // now, create a new database
    if (!temp.createDatabase("chris_db", errMsg)) {
        cout << "Not able to create database: " + errMsg;
        exit(-1);
    } else {
        cout << "Created database.\n";
    }

    // now, create a new set in that database
    if (!temp.createSet("chris_db", "chris_set", "pdb::Supervisor", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit(-1);
    } else {
        cout << "Created set.\n";
    }


    // now, create a new set in that database
    if (!temp.createSet<pdb::Vector<pdb::Handle<pdb::Employee>>>(
            "chris_db", "output_set1", errMsg)) {
        cout << "Not able to create set: " + errMsg;
        exit(-1);
    } else {
        cout << "Created set.\n";
    }


    if (!temp.removeSet("chris_db", "output_set1", errMsg)) {
        cout << "Not able to remove set: " + errMsg;
        exit(-1);
    } else {
        cout << "Removed set.\n";
    }

    if (!temp.removeSet("chris_db", "chris_set", errMsg)) {
        cout << "Not able to remove set: " + errMsg;
        exit(-1);
    } else {
        cout << "Removed set.\n";
    }
}

#endif
