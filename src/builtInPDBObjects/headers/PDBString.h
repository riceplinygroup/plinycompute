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

#ifndef PDBSTRING_H
#define PDBSTRING_H

#include "Array.h"
#include <iostream>
#include <string>

// PRELOAD %String%

namespace pdb {

class String : public Object {

	Handle <Array <char>> data;
	
public:

	ENABLE_DEEP_COPY

	String ();
	String &operator = (const char *toMe);
	String &operator = (const std :: string &s);
	String (const char *fromMe);
	String (const char* s, size_t n);
	String (const std :: string &s);
	char &operator [] (int whichOne);
	operator std :: string ();
	char *c_str ();
	size_t size ();
	friend std::ostream& operator<< (std::ostream& stream, const String &printMe);
	bool operator == (const String &toMe);
	bool operator == (const char *toMe);
	bool operator == (const std :: string &toMe);
	bool operator != (const String &toMe);
	bool operator != (const std :: string &toMe);
	bool operator != (const char *toMe);
};

}

#include "PDBString.cc"

#endif
