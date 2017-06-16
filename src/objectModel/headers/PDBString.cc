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
#include <string.h>
#include <iostream>
#include <string>
#include "PDBMap.h"

namespace pdb {

inline String :: String () {
	data = makeObject <char> ();
	*(c_str ()) = 0;
}

inline size_t String :: hash () const {
	return hashMe (c_str (), -(data.getExactTypeInfoValue ()) - 1);
}

inline String &String :: operator = (const char *toMe) {
	int len = strlen (toMe) + 1;
	data = makeObjectWithExtraStorage <char> ((len - 1) * sizeof (char));
	data.setExactTypeInfoValue (-len);
	memmove (c_str (), toMe, len);
	return *this;
}

inline String &String :: operator = (const std :: string &s) {
	int len = s.size () + 1;
	data = makeObjectWithExtraStorage <char> ((len - 1) * sizeof (char));
	data.setExactTypeInfoValue (-len);
	memmove (c_str (), s.c_str (), len);
	return *this;
}

inline String :: String (const char *toMe) {
	int len = strlen (toMe) + 1;
	data = makeObjectWithExtraStorage <char> ((len - 1) * sizeof (char));
	data.setExactTypeInfoValue (-len);
	memmove (c_str (), toMe, len);
}

inline String :: String (const char* toMe, size_t n) {
	int len = n + 1;
	data = makeObjectWithExtraStorage <char> ((len - 1) * sizeof (char));
	data.setExactTypeInfoValue (-len);
	memmove (c_str (), toMe, len - 1);
	c_str ()[len - 1] = 0;
}

inline String :: String (const std :: string &s) {
	int len = s.size () + 1;
	data = makeObjectWithExtraStorage <char> ((len - 1) * sizeof (char));
	data.setExactTypeInfoValue (-len);
	memmove (c_str (), s.c_str (), len);
}

inline char &String :: operator [] (int whichOne) {
	return c_str ()[whichOne];
}

inline String :: operator std :: string () const {
	return std :: string (c_str ());
}

inline char *String :: c_str () const {
	return &(*data);
}

inline size_t String :: size () const {
	return -(data.getExactTypeInfoValue ()) - 1;
}

inline std::ostream& operator<< (std::ostream& stream, const String &printMe) {
	char *returnVal = printMe.c_str ();
	stream << returnVal;
	return stream;
}

inline bool String :: operator == (const String &toMe) {
	return strcmp (c_str (), toMe.c_str ()) == 0;
}

inline bool String :: operator == (const std :: string &toMe) {
	return strcmp (c_str (), toMe.c_str ()) == 0;
}

inline bool String :: operator == (const char *toMe) {
	return strcmp (c_str (), toMe) == 0;
}

inline bool String :: operator != (const String &toMe) {
	return strcmp (c_str (), toMe.c_str ()) != 0;
}

inline bool String :: operator != (const std :: string &toMe) {
	return strcmp (c_str (), toMe.c_str ()) != 0;
}

inline bool String :: operator != (const char *toMe) {
	return strcmp (c_str (), toMe) != 0;
}

}

#endif
