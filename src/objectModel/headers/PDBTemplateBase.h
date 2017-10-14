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

#ifndef TEMPLATE_BASE_H
#define TEMPLATE_BASE_H

namespace pdb {

template <class ObjType>
int16_t getTypeID();

// Templates present a bit of a problem for the whole v-table fixing framework.  The issue
// is that v-table fixing is done via class name, but every time someone instantiates a
// template, they get a new class name.  It is not feasible to have someone re-register all
// of those classes.
//
// Our solution is to store the type code of the template argument within the template class
// itself.  Then, we have a couple of methods that use this type code to obtain the correct
// deleter and correct deep copy operation of objects of type ObjType.  These operations can
// be used to correctly delete/copy objects in the tamplete.

class PDBTemplateBase {

private:
    // a number greater than zero indicates a type code associated with a pdb :: Object
    // a number less than zero indicates a non-pdb :: Object for whom we know the size
    // a number equal to zero indicates an error; we have no idea as to the type
    int32_t info;

public:
    PDBTemplateBase();

    PDBTemplateBase& operator=(const PDBTemplateBase& toMe);

    // set up the type
    template <class ObjType>
    void setup();

    // set the info value explicitly
    void set(int32_t toMe);

    // this runs the destructor associated with this type, on the object at deletMe
    void deleteConstituentObject(void* deleteMe) const;

    // do a placement new at target, and then copy source over
    void setUpAndCopyFromConstituentObject(void* target, void* source) const;

    // this returns the size (in bytes) of the object pointed to by forMe... note that
    // in general, the implementation of this will NOT look at the object pointed to by
    // forMe, unless the object is of type pdb :: Array, since pdb :: Array is the *only*
    // variable-sized type
    size_t getSizeOfConstituentObject(void* forMe) const;

    // correctly set the vTable pointer of the object pointed to by forMe
    void setVTablePtr(void* forMe) const;

    // returns true if the type indicated by info is descended from pdb :: Object
    bool descendsFromObject() const;

    // returns the type code
    int16_t getTypeCode() const;

    // returns the exact value of field info
    int32_t getExactTypeInfoValue() const;
};
}

#include "PDBTemplateBase.cc"

#endif
