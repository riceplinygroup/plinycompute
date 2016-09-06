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
 * File: STLScopedAllocator.h
 * Author: Jia
 * May 17, 2016, 16:05
 */

#ifndef STLSCOPEDALLOCATOR_H
#define STLSCOPEDALLOCATOR_H

#include <cstddef>
#include "ScopedAllocator.h"
#include <iostream>

template <class T>
class STLScopedAllocator {
public:
    typedef size_t size_type;
    typedef T* pointer;
    typedef T value_type;
    typedef ptrdiff_t difference_type;
    typedef const T* const_pointer;
    typedef T& reference;
    typedef const T& const_reference;

    STLScopedAllocator(void * rawBytes, size_t size) {
        allocator = make_shared<ScopedAllocator>(rawBytes, size);
    }
    template <class U> STLScopedAllocator(const STLScopedAllocator<U>& other) {
        allocator = other.getAllocator();
    }
    template <class U> struct rebind {
        typedef STLScopedAllocator<U> other;
    };
    pointer allocate(size_type n, const void* /*hint*/ =0) {
        void * retPtr = allocator->_malloc_unsafe(n * sizeof(T));
        //if(n*sizeof(T)!=24) {
          // cout << "Size to allocate:"<< n*sizeof(T)<<"\n";
        //}
        if(retPtr == nullptr) {
           std::bad_alloc exception;
           cout << "Can't allocate more from the page! TODO: add a new page!\n";
           throw exception;
        }
        return (pointer) retPtr;
    }
    void deallocate(pointer p, size_type n) {
        //cout << "Size to deallocate:"<< n*sizeof(T)<<"\n";
        return allocator->_free_unsafe(p);
    }
    void construct (pointer p, const T& val) {
        new (p) T(val);
    }
    void destroy (pointer p) {
        p->~T();
    }
    ScopedAllocatorPtr getAllocator() const {
        return allocator;
    }
private:
    ScopedAllocatorPtr allocator;    
};

template <class T, class U>
bool operator==(const STLScopedAllocator<T>& left, const STLScopedAllocator<U>& right) {
   if(left.getAllocator() == right.getAllocator()) {
      return true;
   } else {
      return false;
   }
}
template <class T, class U>
bool operator!=(const STLScopedAllocator<T>& left, const STLScopedAllocator<U>& right) {
   if(left.getAllocator() != right.getAllocator()) {
       return true;
   } else {
       return false;
   }
}

#endif
