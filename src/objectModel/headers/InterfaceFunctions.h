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

#ifndef INTERFACE_FUNCTIONS_H
#define INTERFACE_FUNCTIONS_H

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>
#include "Object.h"

namespace pdb {

template <class ObjType> class RefCountedObject;
template <class ObjType> class Record;
template <class ObjType> class Handle;

// The way that the whole object infrastructure works is that all Objects are
// allocated using the Allocator class.  The Allocator class has a notion of
// a "current allocation block" which is a block of memory that all allocations
// happen from.  There is only one active "current allocation block".  This
// block is set up using the makeObjectAllocatorBlock () function.
//
// When makeObjectAllocatorBlock () is called many times in succession, it 
// causes the allocation blocks that used to be active to become inactive.
// Whenever an inactive allocation block has no more reachable objects in it,
// it is automatically deallocated, so the user never has to worry about memory
// leaks, as long as the interface is used correctly.  For example, consider
// the following code:
//
// makeObjectAllocatorBlock (1024, false);
//
// for (int i = 0; i < 100; i++) {
//     Handle <Employee> myEmp = makeObject <Employee> (100, 120);
//     Handle <Object> foo = makeObject <Supervisor> (134, 124.5, myEmp);
//     makeObjectAllocatorBlock (1024, false);
// }
//
// What will happen here is that before the loop, we'll have created an
// allocation block.  In the zero'th iteration of the loop, myEmp and foo
// will be written to that block.  
//
// At the call to makeObjectAllocatorBlock () at the end of the loop, the
// current, active allocation block (storing myEmp and foo) becomes inactive.  
// At the end of the loop, the destructors for myEmp and foo are called.  
// This causes that inactive block to have no reachable objects, so it is
// automatically deleted.
//
// The new, active allocation block is then used in the next iteration to
// store the next incarnations of the objects pointed to by myEmp and foo.
// But at the next iteration, the same thing will happen again.
//
// The result of all of this is that after the loop executes, there will
// be no remaining inactive allocation blocks; just a single active block
// (the last one created) and that block will have no Objects stored in 
// it.
//
// The first parameter to makeObjectAllocatorBlock (1024, false) is the number of bytes to
// allocate to write objects to.  The second parameter is how to handle makeObject ()
// calls that fail due to lack of memory.  If throwExceptionOnFail == true, then an 
// exception is thrown when an allocate fails.  If throwExceptionOnFail == false, then
// the resulting object will be equal to the nullptr constant.  
//
// Note that while using exceptions are a bit of a pain in the arse, it is MUCH safer 
// to use throwExceptionOnFail == true as oppsed to == false.  The reason is that
// it is easily for operations (such as deep copies to the allocation block) to run out
// of RAM half way through, in a way that is not transparent to th user.  If this 
// happens, the ultimate return val may be OK, but there can be null pointers embedded in
// the middle of the object, and it is going to be difficult to discover this until it is
// too late.  It is recommended that throwExceptionOnFail == false be used primarily
// in simple cases where the allocation block is going to be used to build a small set
// of fixed-sized objects that one is sure are going to fit in the block.
// 
void makeObjectAllocatorBlock (size_t numBytesIn, bool throwExceptionOnFail);

// This is just like the above function, except that the allocation block is allocated
// by the programmer.  As a result, it is NOT automatically freed once the count of
// the number of objects in the block goes to zero.  After the next call to 
// makeObjectAllocatorBlock () the Allocator will "forget" about the block completely.
// The caller is ultimately responsible for freeing the block.
void makeObjectAllocatorBlock (void *spaceToUse, size_t numBytesIn, bool throwExceptionOnFail);

// this gets a count of the total number of free bytes available in the current
// allocation block.
size_t getBytesAvailableInCurrentAllocatorBlock  ();

// this gets a count of the current number of individual, active objects that
// are present in the current allocation block
unsigned getNumObjectsInCurrentAllocatorBlock  ();

// this gets a count of the current number of individual, active objects that
// are present in the allocation block that houses the object pointed to by
// the supplied handle.  If the allocation block is not known to the Allocator
// (because the memory to the allocation block was supplied to the Allocator
// via a call to makeObjectAllocatorBlock (void *, size_t, bool) and there
// has been a subsequent call to makeObjectAllocatorBlock () that caused the
// allocator to frget that particular block) then the result of the call is
// a zero.  The only other way that the result of the call can be a zero is
// if the Handle forMe is equl to a nullptr.
template <class ObjType>
unsigned getNumObjectsInHomeAllocatorBlock (Handle <ObjType> &forMe);

// like the above, except that it finds the number of objects in the block
// that contains the specified pointer
unsigned getNumObjectsInAllocatorBlock (void *forMe);

// creates and returns a RefCountedObject that is located in the current
// allocation block.  This RefCountedObject can be converted into a handle
// via an assignment to a Handle <ObjType>.  This function is used to get a handle
// to an existing object that may have been allocated (for example) on the stack.
// For example, we might use:
//
// class Supervisor : public Object {...}
//
// Handle <Supervisor> mySup;
// Supervisor temp (134, 124.5, myEmp); // allocate temp on the stack
// mySup = getHandle <Supervisor> (temp);
// 
// Note that, just like the rest of these operations, you can use getHandle ()
// and not worry about where the various objects are allocated.  They will be
// copied and moved around as needed.  So you can allocate an object on the stack,
// get a Handle to it (as above) and then later assign that Handle to a handle
// that is located in an object Obj in the current allocation block... in this 
// case, a deep copy will be performed, as needed, so that all handles in Obj
// will point only to Objects located in the current allocation block.
//
template <class ObjType> 
RefCountedObject <ObjType> *getHandle (ObjType &forMe);

// makes an object, allocating it in the current allocation block, 
// and returns a handle to it.  On failure (not enough RAM) the resulting
// handle == nullptr if the allocator is set up not to throw an exception,
// and an exception is thrown if it is set up this way.
//
// Example usage:
//
// class Supervisor : public Object {...}
//
// Handle <Object> mySup = makeObject <Supervisor> (134, 124.5, myEmp);
//
// Note that it is fine to assign an object create as type A to a Handle<B>
// as long as A is a subclass of B.
//
template <class ObjType, class... Args>
RefCountedObject <ObjType> *makeObject (Args&&... args);

// This is just like the above function, but here the first param to
// makeObjectWithExtraStorage is the number of bytes to pad the object
// with at the end.  This is useful when you want additional storage at
// the end of the object, but it is not clear at compile time how much
// you will need... it is used, for example, to implement the Vector 
// class where we want an Object with a bunch of extra storage at the
// end that stores the Vector's data
template <class ObjType, class... Args>
RefCountedObject <ObjType> *makeObjectWithExtraStorage (size_t extra, Args&&... args);

// This gets a raw, bytewise representation of an object from a Handle.  This
// call is always executed in constant time, so it is fast.  The resulting bytes
// are then easily moved around.  For example, consider the following code:
//
// /* Create an object */
//
// Handle <Supervisor> foo = makeObject <Supervisor> (134, 124.5, myEmp);
//
// /* Use getRecord to get the raw bytes of the object */
//
// Record <Supervisor> *myBytes = getRecord <Supervisor> (foo);
//
// /* Write the object to a file */
//
// int filedesc = open ("testfile", O_WRONLY | O_APPEND);
// write (filedesc, myBytes, myBytes->numBytes);
// close (filedesc);
//
// /* Then, at a later time, get the object back */
//
// filedesc = open ("testfile", O_RDONLY);
// Record <Supervisor> *myNewBytes = (Record <Supervisor> *) malloc (myBytes->numBytes);
// read (filedesc, myNewBytes, myBytes->numBytes);
// Handle <Supervisor> bar = myNewBytes-> getRootObject ();
//
// A nullptr is returned on an error.
//
// While this is super-fast (the call to getRecord runs in constant
// time, regardless of the size of the underlying object), there are 
// three important caveats:
//
// (1) The callee does not own the resulting Record <ObjType> *.
//     It will be deallocated automatically.  In particular, 
//     if the destructor for Handle <ObjType> &forMe is called,
//     or forMe is assigned to new value at a later time,
//     then the resulting Record <ObjType> *can be made invalid.
//     Also, a second call to getRecord () can make the resulting
//     Record <ObjType> * invalid.  However, as long as forMe
//     is not destructed or modified, the resulting 
//     Record <ObjType> * will be valid.
//
// (2) There can be a lot of garbage inside of the resulting 
//     Record <ObjType> *.  In particular, everything that was
//     allocated using the same allocator as forMe will also be
//     contained in the result of the call.  So while this is fast,
//     it can be space-inefficient.
//
// (3) This method will fail (return a null pointer) if the object
//     pointed to by forMe is not housed in an allocation block 
//     managed by the Allocator associated with this thread.
//
template <class ObjType> 
Record <ObjType> *getRecord (Handle <ObjType> &forMe);

// This is like Record <ObjType> *getRecord (Handle <ObjType> &forMe)
// except that the bytes for the object pointed to by forMe are actually
// copied to the space pointed to by putMeHere.  On a failure (because
// there was not enough space available to hold a copy of the object)
// an exception is thrown.
//
// The cost of using this version of getRecord as opposed to the previous
// version is that a deep copy is required, which can be expensive.  The
// benefit is that the resulting Record representation can be much more
// compact, because there are going to be no other objects mixed in with
// the bytes.  Further, this not fail simply because the object pointed to
// by forMe is not located in an allocation block managed by this thread's
// allocator.
//
template <class ObjType>
Record <ObjType> *getRecord (Handle <ObjType> &forMe, void *putMeHere, size_t numBytesAvailable);

// this gets the type ID that the system has assigned to this particular type
template <class ObjType>
int16_t getTypeID ();

// this performs a cast whose safety is not verifiable at compile time---note
// that because of difficulties stemming from the use of shared libraries,
// it is not possible to verify the correctness of the cast at runtime, either.
// So use this operation CAREFULLY!!
template <class OutObjType, class InObjType>
Handle <OutObjType> unsafeCast (Handle <InObjType> &castMe);
/*
template <class TargetType>
class Holder : public Object{
    public:
        Handle<TargetType> child;
        ENABLE_DEEP_COPY
};
*/

//To facilitate deep copy, even in case copyMe is of an abstract class
//added by Jia based on Chris' proposal
template <class TargetType>
Handle <TargetType> deepCopyToCurrentAllocationBlock (Handle <TargetType> &copyMe); 

}

#include "InterfaceFunctions.cc"

#endif
