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

#include "Handle.h"
#include "InterfaceFunctions.h"


#ifndef PDBSTRING_H
#define PDBSTRING_H

#include <iostream>
#include <string>

// the String class has a special type code value reserved for it... even though it is no
// longer a PDB Object.  Now, whenever the PDBTemplateBase sees a type code of String_TYPECODE,
// it knows that it is pointing to a String, and acts accordingly.  No VTable or VTable
// fixing is used.  This has been done so as to optimize the storage for this classs,
// since it is so ubiquitous.  Now, the total storage is a Handle (12B), plus the space
// for the counter associated with the target of the Handle (4B), for 16B.  Previously,
// when this was a PDB Object, the total storage was 8B (VTable pointer) + the Handle (12B) +
// the two counters inside the Array class (which the String was built on; 8B) + the type
// code for the type inside the array (4B) + the VTble pointer for the Array (8B) + the
// counter associated with the target of the Handle (4B), for a total size of 44B.  This
// means that the overhead for the non-PDB-Object string is 64% less than the old implementation.

namespace pdb {

class String {

    char storage[sizeof(Handle<char>)];
    Handle<char>& data() const;

public:
    String();
    ~String();
    String& operator=(const char* toMe);
    String& operator=(const std::string& s);
    String& operator=(const String& s);
    String(const char* fromMe);
    String(const char* s, size_t n);
    String(const std::string& s);
    String(const String& s);
    char& operator[](int whichOne);
    operator std::string() const;
    char* c_str() const;
    size_t size() const;
    size_t hash() const;
    friend std::ostream& operator<<(std::ostream& stream, const String& printMe);
    bool operator==(const String& toMe);
    bool operator==(const char* toMe);
    bool operator==(const std::string& toMe);
    bool operator!=(const String& toMe);
    bool operator!=(const std::string& toMe);
    bool operator!=(const char* toMe);
    bool endsWith(const std::string& suffix);
};
}

#include "PDBString.cc"

#endif
