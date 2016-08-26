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

#ifndef CATALOG_C
#define CATALOG_C

#include "MyDB_Catalog.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

void MyDB_Catalog :: putString (string key, string value) {
	myData [key] = value;
}

void MyDB_Catalog :: putStringList (string key, vector <string> value) {
	string res("");
	for (string s : value) {
		res = res + s + "#";
	}
	myData [key] = res;
}

void MyDB_Catalog :: putInt (string key, int value) {
	ostringstream convert;
	convert << value;
	myData [key] = convert.str ();
}

bool MyDB_Catalog :: getIntList (string key, vector <int> &returnVal) {
	vector <string> myInts;
	bool res = getStringList (key, myInts);
	returnVal.clear ();
	for (string &s : myInts) {
		returnVal.push_back (::atoi (s.c_str ()));
	}
	return res;
}

void MyDB_Catalog :: putIntList (string key, vector <int> value) {
	vector <string> myInts;
	for (int i: value) {
		myInts.push_back (to_string (i));
	}
	putStringList (key, myInts);
}

bool MyDB_Catalog :: getStringList (string key, vector <string> &returnVal) {

	// verify the entry is in the map
	if (myData.count (key) == 0)
		return false;

	// it is, so parse the other side
	string res = myData[key];	
	for (int pos = 0; pos < (int) res.size (); pos = res.find ("#", pos + 1) + 1) {
		string temp = res.substr (pos, res.find ("#", pos + 1) - pos);
		returnVal.push_back (temp);
	}
	return true;
}

bool MyDB_Catalog :: getString (string key, string &res) {
	if (myData.count (key) == 0)
		return false;

	res = myData[key];
	return true;
}

bool MyDB_Catalog :: getInt (string key, int &value) {

	// verify the entry is in the map
	if (myData.count (key) == 0)
		return false;

	// it is, so convert it to an int
	string :: size_type sz;
	try {
		value = std::stoi (myData [key], &sz);

	// exception means that we could not convert
	} catch (...) {
		return false;
	}
	
	return true;
}

MyDB_Catalog :: MyDB_Catalog (string fNameIn) {

	// remember the catalog name
	fName = fNameIn;

	// try to open the file
	string line;
	ifstream myfile (fName);

	// if we opened it, read the contents
	if (myfile.is_open()) {

		// loop through all of the lines
    		while (getline (myfile,line)) {

			// find how to cut apart the string
			int firstPipe, secPipe, lastPipe;
			firstPipe = line.find ("|");
			secPipe = line.find ("|", firstPipe + 1); 
			lastPipe = line.find ("|", secPipe + 1); 

			// if there is an error, don't add anything
			if (firstPipe >= (int) line.size () || secPipe >= (int) line.size () || lastPipe >= (int) line.size ())
				continue;

			// and add the pair
			myData [line.substr (firstPipe + 1, secPipe - firstPipe - 1)] = 
				line.substr (secPipe + 1, lastPipe - secPipe - 1);
		}
		myfile.close();
	}
}

MyDB_Catalog :: ~MyDB_Catalog () {

	// just save the contents
	save ();
}

void MyDB_Catalog :: save () {

	ofstream myFile (fName, ofstream::out | ofstream::trunc);
	if (myFile.is_open()) {
		for (auto const &ent : myData) {
			myFile << "|" << ent.first << "|" << ent.second << "|\n";
		}
	}
}

#endif


