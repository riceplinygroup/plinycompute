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

#ifndef TABLE_C
#define TABLE_C

#include "MyDB_Table.h"

MyDB_Table :: MyDB_Table (string name, string storageLocIn) {
	tableName = name;
	storageLoc = storageLocIn;
}

MyDB_Table :: ~MyDB_Table () {}

string &MyDB_Table :: getName () {
	return tableName;
}

string &MyDB_Table :: getStorageLoc () {
	return storageLoc;
}

#endif

