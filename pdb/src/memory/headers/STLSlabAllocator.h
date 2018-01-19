/*
 * File: STLSlabAllocator.h
 * Author: Jia
 * May 20, 2016, 10:57
 */

#ifndef STLSLABALLOCATOR_H
#define STLSLABALLOCATOR_H

#include <cstddef>
#include "SlabAllocator.h"
#include <iostream>

template <class T>
class STLSlabAllocator {
public:
    typedef size_t size_type;
    typedef T* pointer;
    typedef T value_type;
    typedef ptrdiff_t difference_type;
    typedef const T* const_pointer;
    typedef T& reference;
    typedef const T& const_reference;

    STLSlabAllocator(size_t size) {
        allocator = make_shared<SlabAllocator>(size, true);
    }
    STLSlabAllocator(void* memPool, size_t size) {
        allocator = make_shared<SlabAllocator>(memPool, size, true);
    }

    template <class U>
    STLSlabAllocator(const STLSlabAllocator<U>& other) {
        allocator = other.getAllocator();
    }
    template <class U>
    struct rebind {
        typedef STLSlabAllocator<U> other;
    };
    pointer allocate(size_type n, const void* /*hint*/ = 0) {
        void* retPtr = allocator->do_slabs_alloc(n * sizeof(T));
        // if(n*sizeof(T)!=24) {
        // cout << "Size to allocate:"<< n*sizeof(T)<<"\n";
        //}
        if (retPtr == nullptr) {
            std::bad_alloc exception;
            cout << "Can't allocate more from the page! TODO: add a new page!\n";
            throw exception;
        }
        return (pointer)retPtr;
    }
    void deallocate(pointer p, size_type n) {
        // cout << "Size to deallocate:"<< n*sizeof(T)<<"\n";
        return allocator->do_slabs_free(p, n * sizeof(T));
    }
    void construct(pointer p, const T& val) {
        new (p) T(val);
    }
    void destroy(pointer p) {
        p->~T();
    }
    SlabAllocatorPtr getAllocator() const {
        return allocator;
    }

private:
    SlabAllocatorPtr allocator;
};

template <class T, class U>
bool operator==(const STLSlabAllocator<T>& left, const STLSlabAllocator<U>& right) {
    if (left.getAllocator() == right.getAllocator()) {
        return true;
    } else {
        return false;
    }
}
template <class T, class U>
bool operator!=(const STLSlabAllocator<T>& left, const STLSlabAllocator<U>& right) {
    if (left.getAllocator() != right.getAllocator()) {
        return true;
    } else {
        return false;
    }
}

#endif
