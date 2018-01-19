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

#ifndef OBJECT_CC
#define OBJECT_CC

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>

namespace pdb {

// empty placement new
inline void* Object::operator new(std::size_t count, void* ptr) {
    return ptr;
}

inline void* Object::operator new(size_t sz, const std::nothrow_t& tag) {

    std::cerr << "This should not be called!!  We are in overloaded new.\n";
    std::cerr << "Instead, a placement new should be done by the handle.\n";
    exit(1);
}

inline void* Object::operator new(size_t sz) {

    std::cerr << "This should not be called!!  We are in overloaded new.\n";
    std::cerr << "Instead, a placement new should be done by the handle.\n";
    exit(1);
}

inline void Object::operator delete(void* me) {

    std::cerr << "This should not be called!!  We are in overloaded delete.\n";
    std::cerr << "Instead, the deletion should be done by the handle.\n";
    exit(1);
}

inline void Object::setVTablePtr(void* setToMe) {
    // This assumes that the vTable pointer is located at the very beginning of the object.
    // This has been verified on several compilers.

    // this will NOT set the vTable pointer to be a null pointer
    if (setToMe == nullptr)
        return;

    int** temp = (int**)this;
    *temp = (int*)setToMe;
}

inline void* Object::getVTablePtr() {
    int** temp = (int**)this;
    return *temp;
}

inline void Object::setUpAndCopyFrom(void* target, void* source) const {
    std::cerr
        << "Bad: you are trying to do a deep Object copy without the ENABLE_DEEP_COPY macro.\n";
    int* a = 0;
    *a = 12;
    exit(1);
}

inline void Object::deleteObject(void* deleteMe) {
    std::cerr
        << "Bad: you are trying to do a deep Object copy without the ENABLE_DEEP_COPY macro.\n";
    int* a = 0;
    *a = 12;
    exit(1);
}

inline size_t Object::getSize(void* forMe) {
    std::cerr
        << "Bad: you are trying to do a deep Object copy without the ENABLE_DEEP_COPY macro.\n";
    int* a = 0;
    *a = 12;
    exit(1);
}
}

#endif
