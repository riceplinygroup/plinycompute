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

#ifndef TABLE_H
#define TABLE_H

#include <memory>
#include <string>

// create a smart pointer for database tables
using namespace std;
class MyDB_Table;
typedef shared_ptr<MyDB_Table> MyDB_TablePtr;

// this class encapsulates the notion of a database table
// DO NOT MODIFY!
class MyDB_Table {

public:
    // creates a new table with the given name, at the given storage location
    MyDB_Table(string name, string storageLoc);

    // get the name of the table
    string& getName();

    // get the storage location of the table
    string& getStorageLoc();

    // kill the dude
    ~MyDB_Table();

private:
    // the name of the table
    string tableName;

    // the location where it is stored
    string storageLoc;
};

#endif
