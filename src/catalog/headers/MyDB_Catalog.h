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

#ifndef CATALOG_H
#define CATALOG_H

#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace std;

// create a smart pointer for the catalog
using namespace std;
class MyDB_Catalog;
typedef shared_ptr <MyDB_Catalog> MyDB_CatalogPtr;

// this encapsulates a simple little key-value store
class MyDB_Catalog {

public:

	// these functions search the catalog for the specified key, and
	// they return the value stored in the catalog using the specified
	// type.  A true is returned on success.  A false is returned on 
	// error (the key is not found, or the value cannot be cast to the
	// specified type)
	bool getInt (string key, int &value);
	bool getIntList (string key, vector <int> &value);
	bool getLong (string key, long &value);
	bool getLongList (string key, vector <long> &value);
	bool getString (string key, string &value);
	bool getStringList (string key, vector <string> &value);
	bool getDouble (string key, double &value);
	bool getDoubleList (string key, vector <double> &value);

	// these functions add a new (key, value) pair into the catalog.
	// If the key is already in the catalog, then its value is replaced.
	void putString (string key, string value);
	void putStringList (string key, vector <string> value);
	void putInt (string key, int value);
	void putIntList (string key, vector <int> value);
	void putDouble (string key, double value);
	void putDoubleList (string key, vector <double> value);
	void putLong (string key, vector <long> value);

	// creates an instance of the catalog.  If the specified file does
	// not exist, it is created.  Otherwise, the existing file is 
	// opened.
	MyDB_Catalog (string fName);

	// closes the catalog, and saves the contents.
	~MyDB_Catalog ();

	// saves any updates to the catalog
	void save ();

private:

	// the name of the catalog file
	string fName;

	// the map that stores the catalog's contents
	map <string, string> myData;
};

#endif
