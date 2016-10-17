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

#ifndef PDBSTRING_CC
#define PDBSTRING_CC

#include "PDBString.h"
#include "PDBMap.h"
#include <string.h>
#include <iostream>
#include <string>

namespace pdb {

inline String :: String () {}

inline size_t String :: hash () const {
	return hashMe (data->c_ptr (), data->numUsedSlots () - 1);
}

inline String &String :: operator = (const char *toMe) {
	int len = strlen (toMe) + 1;
	data = makeObjectWithExtraStorage <Array <char>> (len * sizeof (char), len);
	memmove (data->c_ptr (), toMe, len);
	data->setUsed (len);
	return *this;
}

inline String &String :: operator = (const std :: string &s) {
	int len = s.size () + 1;
	data = makeObjectWithExtraStorage <Array <char>> (len * sizeof (char), len);
	memmove (data->c_ptr (), s.c_str (), len);
	data->setUsed (len);
	return *this;
}

inline String :: String (const char *toMe) {
	int len = strlen (toMe) + 1;
	data = makeObjectWithExtraStorage <Array <char>> (len * sizeof (char), len);
	memmove (data->c_ptr (), toMe, len);
	data->setUsed (len);
}

inline String :: String (const char* toMe, size_t n) {
	int len = n + 1;
	data = makeObjectWithExtraStorage <Array <char>> (len * sizeof (char), len);
	memmove (data->c_ptr (), toMe, len - 1);
	data->c_ptr ()[len - 1] = 0;
	data->setUsed (len);
}

inline String :: String (const std :: string &s) {
	int len = s.size () + 1;
	data = makeObjectWithExtraStorage <Array <char>> (len * sizeof (char), len);
	memmove (data->c_ptr (), s.c_str (), len);
	data->setUsed (len);
}

inline char &String :: operator [] (int whichOne) {
	return data->c_ptr ()[whichOne];
}

inline String :: operator std :: string () const {
	return std :: string (data->c_ptr ());
}

inline char *String :: c_str () {
	return data->c_ptr ();
}

inline size_t String :: size () const {
	return data->numUsedSlots () - 1;
}

inline std::ostream& operator<< (std::ostream& stream, const String &printMe) {
	char *returnVal = printMe.data->c_ptr ();
	stream << returnVal;
	return stream;
}

inline bool String :: operator == (const String &toMe) {
	return strcmp (data->c_ptr (), toMe.data->c_ptr ()) == 0;
}

inline bool String :: operator == (const std :: string &toMe) {
	return strcmp (data->c_ptr (), toMe.c_str ()) == 0;
}

inline bool String :: operator == (const char *toMe) {
	return strcmp (data->c_ptr (), toMe) == 0;
}

inline bool String :: operator != (const String &toMe) {
	return strcmp (data->c_ptr (), toMe.data->c_ptr ()) != 0;
}

inline bool String :: operator != (const std :: string &toMe) {
	return strcmp (data->c_ptr (), toMe.c_str ()) != 0;
}

inline bool String :: operator != (const char *toMe) {
	return strcmp (data->c_ptr (), toMe) != 0;
}
}

#endif
