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
/*
 * File: STLScopedAllocator.cc
 * Author: Jia
 * May 17, 2016, 16:05
 */

#ifndef SCOPEDALLOCATOR_CC
#define SCOPEDALLOCATOR_CC

#include "STLScopedAllocator.h"
/*
template <class T>
STLScopedAllocator<T>::STLScopedAllocator(void * rawBytes, size_t size) {
    this->allocator = make_shared<ScopedAllocator>(rawBytes, size);
}
*/
/*
template<class T> template<class U>
STLScopedAllocator<T>::STLScopedAllocator(const STLScopedAllocator<U>& other) {
    this->allocator = other.getAllocator();
}
*/

/*
template<class T>
pointer STLScopedAllocator<T>::allocate(size_type n) {
    return (T *) allocator->_malloc_unsafe(n);
}
*/
/*
template<class T>
void STLScopedAllocator<T>::deallocate(pointer p, size_type n) {
    return allocator->_free_unsafe(p);
}
*/
/*
template<class T>
ScopedAllocatorPtr STLScopedAllocator<T>::getAllocator() {
    return this->allocator;
}

template <class T, class U>
bool operator==(const STLScopedAllocator<T>& left, const STLScopedAllocator<U>& right) {
    if(left->getAllocator() == right->getAllocator()) {
        return true;
    } else {
        return false;
    }
}

template <class T, class U>
bool operator!=(const STLScopedAllocator<T>& left, const STLScopedAllocator<U>& right) {
    if(left->getAllocator() != right->getAllocator()) {
        return true;
    } else {
        return false;
    }
}
*/
#endif
