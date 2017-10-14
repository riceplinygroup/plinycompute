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

#ifndef HANDLE_CC
#define HANDLE_CC

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>

#include "PDBDebug.h"
#include "TypeName.h"
#include "Handle.h"
#include "RefCountedObject.h"

namespace pdb {

#define CHAR_PTR(c) ((char*)c)

template <class ObjType>
Handle<ObjType>::Handle() {
    typeInfo.setup<ObjType>();
    offset = -1;
}

template <class ObjType>
int16_t Handle<ObjType>::getTypeCode() {
    return typeInfo.getTypeCode();
}

template <class ObjType>
int32_t Handle<ObjType>::getExactTypeInfoValue() const {
    return typeInfo.getExactTypeInfoValue();
}

template <class ObjType>
void Handle<ObjType>::setExactTypeInfoValue(int32_t toMe) {
    typeInfo.set(toMe);
}

template <class ObjType>
Handle<ObjType>::~Handle() {

    // if we are not null, then dec the reference count
    if (!isNullPtr() && getAllocator().isManaged(getTarget())) {
        getTarget()->decRefCount(typeInfo);
    }
}

template <class ObjType>
unsigned Handle<ObjType>::getRefCount() {

    // if we are not null, then dec the reference count
    if (!isNullPtr() && getAllocator().isManaged(getTarget())) {
        return getTarget()->getRefCount();
    }

    // returned if we have no reference count
    return 9999999;
}

template <class ObjType>
void Handle<ObjType>::emptyOutContainingBlock() {

    if (!isNullPtr()) {

        // go ahead and free his block
        getAllocator().emptyOutBlock(getTarget());

        // and set us so that we are a null pointer
        offset = -1;
    } else {
        std::cout << "This seems bad.  I don't think that you should be emptying the containing "
                     "block for a nullptr.\n";
    }
}

template <class ObjType>
Handle<ObjType>::Handle(GenericHandle rhs) {
    typeInfo = rhs.getMyBase();
    offset = -1;
}


template <class ObjType>
Handle<ObjType>::Handle(const std::nullptr_t rhs) {
    typeInfo.setup<Nothing>();
    offset = -1;
}

template <class ObjType>
Handle<ObjType> Handle<ObjType>::copyTargetToCurrentAllocationBlock() {

    // if the current allocator contains us, then just return ourselves
    if (getAllocator().contains(getTarget())) {
        return *this;
    }

    // otherwise, make a new handle and do the assignment via a deep copy
    Handle<ObjType> returnVal;

// get the space... allocate and set up the reference count before it
#ifdef DEBUG_OBJECT_MODEL
    void* space =
        getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE + sizeof(ObjType), typeInfo.getTypeCode());
#else
    void* space = getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE + sizeof(ObjType));
#endif
    // see if there was not enough RAM
    if (space == nullptr) {
        PDB_COUT << "ERROR: Not enough memory when invoking copyTargetToCurrentAllocationBlock() "
                    "in Handle.cc"
                 << std::endl;
        returnVal.offset = -1;
        throw myException;
    }

    // point the new handle at the space
    returnVal.offset = CHAR_PTR(space) - CHAR_PTR(&returnVal);
    returnVal.getTarget()->setRefCount(1);

    // copy over the object
    returnVal.typeInfo = typeInfo;
    try {
        if (typeInfo.getTypeCode() == 0) {
            PDB_COUT << "copyTargetToCurrentAllocationBlock: typeInfo = 0 before "
                        "setUpAndCopyFromConstituentObject"
                     << std::endl;
        }
        typeInfo.setUpAndCopyFromConstituentObject(returnVal.getTarget()->getObject(),
                                                   getTarget()->getObject());
    } catch (NotEnoughSpace& n) {
        PDB_COUT << "ERROR: Not enough memory when invoking copyTargetToCurrentAllocationBlock() "
                    "in Handle.cc"
                 << std::endl;
        returnVal.getTarget()->decRefCount(typeInfo);
        returnVal.offset = -1;
        throw n;
    }
    // and reutrn him
    return returnVal;
}

#define GET_OLD_TARGET                              \
    RefCountedObject<ObjType>* oldTarget = nullptr; \
    if (!isNullPtr())                               \
        oldTarget = getTarget();                    \
    PDBTemplateBase oldTypeInfo = typeInfo;
#define DEC_OLD_REF_COUNT     \
    if (oldTarget != nullptr) \
    oldTarget->decRefCount(oldTypeInfo)

template <class ObjType>
Handle<ObjType>& Handle<ObjType>::operator=(const std::nullptr_t rhs) {
    GET_OLD_TARGET;
    DEC_OLD_REF_COUNT;
    offset = -1;
    return *this;
}

template <class ObjType>
bool Handle<ObjType>::isNullPtr() const {
    return (offset == -1);
}

/***************************************************************************/
/* There are eight different cases for assign/copy construction on Handles */
/*                                                                         */
/* 1. Copy construct from RefCountedObject of same Object type             */
/* 2. Copy construct from RefCountedObject of diff Object type             */
/* 3. Copy construct from Handle of same Object type                       */
/* 4. Copy construct from Handle of diff Object type                       */
/* 5. Assignment from RefCountedObject of same Object type                 */
/* 6. Assignment from RefCountedObject of diff Object type                 */
/* 7. Assignment from Handle of same Object type                           */
/* 8. Assignment from Handle of diff Object type                           */
/***************************************************************************/

/***************************************/
/* Here are the four copy constructors */
/***************************************/

/*************************************************************/
/* Here are the two copy constructors from RefCountedObjects */
/*************************************************************/

template <class ObjType>
Handle<ObjType>::Handle(const RefCountedObject<ObjType>* fromMe) {

    // set up the type info for this guy
    typeInfo.setup<ObjType>();

    if (fromMe == nullptr) {
        offset = -1;
        return;
    }

    // if the RHS is not in the current allocator, but the handle is, then
    // we need to copy it over using a deep copy
    if (!getAllocator().contains((void*)fromMe) && getAllocator().contains(this)) {
#ifdef DEBUG_OBJECT_MODEL
        // get the space... allocate and set up the reference count before it
        void* space = getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE + sizeof(ObjType),
                                            typeInfo.getTypeCode());
#else
        void* space = getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE + sizeof(ObjType));
#endif
        // see if there was not enough RAM
        if (space == nullptr) {
            std::cout << "ERROR: Not enough memory when invoking Handle (const RefCountedObject "
                         "<ObjType>) in Handle.cc"
                      << std::endl;
            offset = -1;
            throw myException;
        }

        offset = CHAR_PTR(space) - CHAR_PTR(this);

        getTarget()->setRefCount(1);

        // copy over the object
        try {
            typeInfo.setUpAndCopyFromConstituentObject(getTarget()->getObject(),
                                                       fromMe->getObject());
        } catch (NotEnoughSpace& n) {
            PDB_COUT << "ERROR: Not enough memory when invoking Handle(const "
                        "RefCountedObject<ObjType>) in Handle.cc"
                     << std::endl;
            getTarget()->decRefCount(typeInfo);
            offset = -1;
            throw n;
        }

        // if the RHS is not in the current allocator or the LHS handle is not in the
        // current allocator, then we just do a shallow copy
    } else {

        // set the offset
        offset = CHAR_PTR(fromMe) - CHAR_PTR(this);

        getTarget()->incRefCount();
    }
}

class String;

template <class ObjType, class ObjTypeTwo>
auto convert(ObjType*, ObjTypeTwo*) -> std::enable_if_t<
    std::is_base_of<ObjType, ObjTypeTwo>::value ||
        (std::is_base_of<ObjType, Object>::value && std::is_base_of<String, ObjTypeTwo>::value) ||
        (std::is_base_of<ObjType, Object>::value && std::is_base_of<HandleBase, ObjTypeTwo>::value),
    int> {
    return 7;
}

template <class ObjType>
template <class ObjTypeTwo>
Handle<ObjType>::Handle(const RefCountedObject<ObjTypeTwo>* fromMe) {

    ObjType* one = nullptr;
    ObjTypeTwo* two = nullptr;
    convert(one, two);  // this line will not compile with a bad assignment

    // set up the type info for this guy
    typeInfo.setup<ObjTypeTwo>();

    if (fromMe == nullptr) {
        offset = -1;
        return;
    }

    // if the RHS is not in the current allocator, but the handle is, then
    // we need to copy it over using a deep copy
    if (!getAllocator().contains((void*)fromMe) && getAllocator().contains(this)) {

// get the space... allocate and set up the reference count before it
#ifdef DEBUG_OBJECT_MODEL
        void* space = getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE + sizeof(ObjTypeTwo),
                                            typeInfo.getTypeCode());
#else
        void* space = getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE + sizeof(ObjTypeTwo));
#endif
        // see if there was not enough RAM
        if (space == nullptr) {
            std::cout << "ERROR: Not enough memory when invoking Handle (const RefCountedObject "
                         "<ObjTypeTwo>) in Handle.cc"
                      << std::endl;
            offset = -1;
            throw myException;
        }

        offset = CHAR_PTR(space) - CHAR_PTR(this);

        getTarget()->setRefCount(1);

        // copy over the object; use a virtual method so that we get everything set up and copied
        // correctly
        try {
            typeInfo.setUpAndCopyFromConstituentObject(getTarget()->getObject(),
                                                       fromMe->getObject());
        } catch (NotEnoughSpace& n) {
            PDB_COUT << "ERROR: Not enough memory when invoking Handle(const "
                        "RefCountedObject<ObjTypeTwo>) in Handle.cc"
                     << std::endl;
            getTarget()->decRefCount(typeInfo);
            offset = -1;
            throw n;
        }

        // if the RHS is not in the current allocator or the LHS handle is not in the
        // current allocator, then we just do a shallow copy
    } else {

        // set the offset
        offset = CHAR_PTR(fromMe) - CHAR_PTR(this);

        getTarget()->incRefCount();
    }
}

/***************************************************/
/* Here are the two copy constructors from Handles */
/***************************************************/

template <class ObjType>
Handle<ObjType>::Handle(const Handle<ObjType>& fromMe) {

    // if we got a null pointer as the RHS, then we are the RHS
    if (fromMe.isNullPtr()) {
        offset = -1;
        return;
    }

    typeInfo = fromMe.typeInfo;

    // if the RHS is not in the current allocator, but the handle is, then
    // we need to copy it over using a deep copy
    if (!getAllocator().contains(fromMe.getTarget()) && getAllocator().contains(this)) {

// get the space... allocate and set up the reference count before it
#ifdef DEBUG_OBJECT_MODEL
        void* space = getAllocator().getRAM(
            REF_COUNT_PREAMBLE_SIZE +
                typeInfo.getSizeOfConstituentObject(fromMe.getTarget()->getObject()),
            typeInfo.getTypeCode());
#else
        void* space = getAllocator().getRAM(
            REF_COUNT_PREAMBLE_SIZE +
            typeInfo.getSizeOfConstituentObject(fromMe.getTarget()->getObject()));
#endif
        // see if there was not enough RAM
        if (space == nullptr) {
            std::cout << "ERROR: Not enough memory when invoking Handle(const Handle<ObjType>) in "
                         "Handle.cc"
                      << std::endl;
            offset = -1;
            throw myException;
        }

        offset = CHAR_PTR(space) - CHAR_PTR(this);

        // set the reference count to one then decrement the old ref count
        getTarget()->setRefCount(1);

        // copy over the object; use a virtual method so that we get everything set up and copied
        // correctly
        try {
            typeInfo.setUpAndCopyFromConstituentObject(getTarget()->getObject(),
                                                       fromMe.getTarget()->getObject());
        } catch (NotEnoughSpace& n) {
            PDB_COUT << "ERROR: Not enough memory when invoking Handle(const Handle<ObjType>) in "
                        "Handle.cc"
                     << std::endl;
            getTarget()->decRefCount(typeInfo);
            offset = -1;
            throw n;
        }
        // if the RHS is not in the current allocator or the LHS handle is not in the
        // current allocator, then we just do a shallow copy
    } else {

        // set the offset
        offset = CHAR_PTR(fromMe.getTarget()) - CHAR_PTR(this);

        getTarget()->incRefCount();
    }
}

template <class ObjType>
template <class ObjTypeTwo>
Handle<ObjType>::Handle(const Handle<ObjTypeTwo>& fromMe) {

    ObjType* one = nullptr;
    ObjTypeTwo* two = nullptr;
    convert(one, two);  // this line will not compile with a bad assignment

    // if we got a null pointer as the RHS, then we are the RHS
    if (fromMe.isNullPtr()) {
        offset = -1;
        return;
    }

    typeInfo = fromMe.typeInfo;

    // if the RHS is not in the current allocator, but the handle is, then
    // we need to copy it over using a deep copy
    if (!getAllocator().contains(fromMe.getTarget()) && getAllocator().contains(this)) {

// get the space... allocate and set up the reference count before it
#ifdef DEBUG_OBJECT_MODEL
        void* space = getAllocator().getRAM(
            REF_COUNT_PREAMBLE_SIZE +
                typeInfo.getSizeOfConstituentObject(fromMe.getTarget()->getObject()),
            typeInfo.getTypeCode());
#else
        void* space = getAllocator().getRAM(
            REF_COUNT_PREAMBLE_SIZE +
            typeInfo.getSizeOfConstituentObject(fromMe.getTarget()->getObject()));
#endif
        // see if there was not enough RAM
        if (space == nullptr) {
            std::cout << "ERROR: Not enough memory when invoking Handle(const Handle<ObjTypeTwo>) "
                         "in Handle.cc"
                      << std::endl;
            offset = -1;
            throw myException;
        }

        offset = CHAR_PTR(space) - CHAR_PTR(this);

        getTarget()->setRefCount(1);

        // copy over the object; use a virtual method so that we get everything set up and copied
        // correctly
        try {
            typeInfo.setUpAndCopyFromConstituentObject(getTarget()->getObject(),
                                                       fromMe.getTarget()->getObject());
        } catch (NotEnoughSpace& n) {
            PDB_COUT << "ERROR: Not enough memory when invoking Handle(const Handle<ObjTypeTwo>) "
                        "in Handle.cc"
                     << std::endl;
            getTarget()->decRefCount(typeInfo);
            offset = -1;
            throw n;
        }
        // if the RHS is not in the current allocator or the LHS handle is not in the
        // current allocator, then we just do a shallow copy
    } else {

        // set the offset
        offset = CHAR_PTR(fromMe.getTarget()) - CHAR_PTR(this);

        getTarget()->incRefCount();
    }
}

/******************************************/
/* Here are the four assignment operators */
/******************************************/

/****************************************************************/
/* Here are the two assignment operators from RefCountedObjects */
/****************************************************************/

template <class ObjType>
Handle<ObjType>& Handle<ObjType>::operator=(const RefCountedObject<ObjType>* fromMe) {
    // get the thing that we used to point to
    GET_OLD_TARGET;

    // set up the type info for this guy
    typeInfo.setup<ObjType>();

    if (fromMe == nullptr) {
        std::cout << "ERROR: fromMe is nullptr when invoking operator = (const RefCountedObject "
                     "<ObjType>*) in Handle.cc"
                  << std::endl;
        DEC_OLD_REF_COUNT;
        offset = -1;
        return *this;
    }

    // if the RHS is not in the current allocator, but the handle is, then
    // we need to copy it over using a deep copy
    if (!getAllocator().contains((void*)fromMe) && getAllocator().contains(this)) {

// get the space... allocate and set up the reference count before it
#ifdef DEBUG_OBJECT_MODEL
        void* space = getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE + sizeof(ObjType),
                                            typeInfo.getTypeCode());
#else
        void* space = getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE + sizeof(ObjType));
#endif
        // see if there was not enough RAM
        if (space == nullptr) {
            std::cout << "ERROR: not enough memory when invoking operator = (const "
                         "RefCountedObject <ObjType>*) in Handle.cc"
                      << std::endl;
            DEC_OLD_REF_COUNT;
            offset = -1;
            throw myException;
        }

        offset = CHAR_PTR(space) - CHAR_PTR(this);

        // set the reference count to one then decrement the old ref count
        getTarget()->setRefCount(1);

        // copy over the object; use a virtual method so that we get everything set up and copied
        // correctly
        try {
            if (typeInfo.getTypeCode() == 0) {
                PDB_COUT << "operator=: typeInfo = 0 before setUpAndCopyFromConstituentObject"
                         << std::endl;
            }
#ifdef DEBUG_DEEP_COPY
            if (typeInfo.getTypeCode() > 126) {
                PDB_COUT << "typeInfo=" << typeInfo.getTypeCode() << std::endl;
            }
#endif
            typeInfo.setUpAndCopyFromConstituentObject(getTarget()->getObject(),
                                                       fromMe->getObject());
        } catch (NotEnoughSpace& n) {
            PDB_COUT << "ERROR: Not enough memory when invoking operator = (const "
                        "RefCountedObject<ObjType>*) in Handle.cc"
                     << std::endl;
            DEC_OLD_REF_COUNT;
            getTarget()->decRefCount(typeInfo);
            offset = -1;
            throw n;
        }
        DEC_OLD_REF_COUNT;

        // if the RHS is not in the current allocator or the LHS handle is not in the
        // current allocator, then we just do a shallow copy
    } else {
#ifdef DEBUG_OBJECT_MODEL
        if (!getAllocator().contains((void*)fromMe) && !getAllocator().contains(this)) {
            std::cout << "#################################################" << std::endl;
            std::cout << "Both LHS and RHS are not in current block" << std::endl;
            std::cout << "RHS typeinfo =" << typeInfo.getTypeCode() << std::endl;
            std::cout << "#################################################" << std::endl;
        }
#endif


        // set the offset
        offset = CHAR_PTR(fromMe) - CHAR_PTR(this);
        // std :: cout << "offset=" << offset << std :: endl;
        // increment the reference count then decrement the old ref count...
        // it is important to do it in this order to correctly handle self-
        // assignments without accidentally setting the count to zero
        getTarget()->incRefCount();
        // std :: cout << "to dec old ref count" << std :: endl;
        DEC_OLD_REF_COUNT;
        // std :: cout << "to return" << std :: endl;
    }

    return *this;
}

template <class ObjType>
template <class ObjTypeTwo>
Handle<ObjType>& Handle<ObjType>::operator=(const RefCountedObject<ObjTypeTwo>* fromMe) {

    ObjType* one = nullptr;
    ObjTypeTwo* two = nullptr;
    convert(one, two);  // this line will not compile with a bad assignment

    // get the thing that we used to point to
    GET_OLD_TARGET;

    // set up the type info for this guy
    typeInfo.setup<ObjTypeTwo>();

    if (fromMe == nullptr) {
        DEC_OLD_REF_COUNT;
        offset = -1;
        return *this;
    }

    // if the RHS is not in the current allocator, but the handle is, then
    // we need to copy it over using a deep copy
    if (!getAllocator().contains((void*)fromMe) && getAllocator().contains(this)) {

// get the space... allocate and set up the reference count before it
#ifdef DEBUG_OBJECT_MODEL
        void* space = getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE + sizeof(ObjTypeTwo),
                                            typeInfo.getTypeCode());
#else
        void* space = getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE + sizeof(ObjTypeTwo));
#endif
        // see if there was not enough RAM
        if (space == nullptr) {
            std::cout << "ERROR: Not enough memory when invoking operator = (const "
                         "RefCountedObject<ObjTypeTwo>*) in Handle.cc"
                      << std::endl;
            DEC_OLD_REF_COUNT;
            offset = -1;
            throw myException;
        }

        offset = CHAR_PTR(space) - CHAR_PTR(this);

        // set the reference count to one then decrement the old ref count
        getTarget()->setRefCount(1);

        // copy over the object; use a virtual method so that we get everything set up and copied
        // correctly
        try {
            typeInfo.setUpAndCopyFromConstituentObject(getTarget()->getObject(),
                                                       fromMe->getObject());
        } catch (NotEnoughSpace& n) {
            PDB_COUT << "ERROR: Not enough memory when invoking operator = (const "
                        "RefCountedObject<ObjTypeTwo>*) in Handle.cc"
                     << std::endl;
            getTarget()->decRefCount(typeInfo);
            DEC_OLD_REF_COUNT;
            offset = -1;
            throw n;
        }
        DEC_OLD_REF_COUNT;

        // if the RHS is not in the current allocator or the LHS handle is not in the
        // current allocator, then we just do a shallow copy
    } else {
#ifdef DEBUG_OBJECT_MODEL
        if (!getAllocator().contains((void*)fromMe) && !getAllocator().contains(this)) {
            std::cout << "#################################################" << std::endl;
            std::cout << "Both LHS and RHS are not in current block" << std::endl;
            std::cout << "RHS typeinfo =" << typeInfo.getTypeCode() << std::endl;
            std::cout << "#################################################" << std::endl;
        }
#endif

        // set the offset
        offset = CHAR_PTR(fromMe) - CHAR_PTR(this);

        // increment the reference count then decrement the old ref count...
        // it is important to do it in this order to correctly handle self-
        // assignments without accidentally setting the count to zero
        getTarget()->incRefCount();
        DEC_OLD_REF_COUNT;
    }

    return *this;
}

/******************************************************/
/* Here are the two assignment operators from Handles */
/******************************************************/

template <class ObjType>
Handle<ObjType>& Handle<ObjType>::operator=(const Handle<ObjType>& fromMe) {

    // get the thing that we used to point to
    GET_OLD_TARGET;

    // if we got a null pointer as the RHS, then we are the RHS
    if (fromMe == nullptr) {
        DEC_OLD_REF_COUNT;
        offset = -1;
        return *this;
    }

    typeInfo = fromMe.typeInfo;

    // if the RHS is not in the current allocator, but the handle is, then
    // we need to copy it over using a deep copy
    if (!getAllocator().contains(fromMe.getTarget()) && getAllocator().contains(this)) {

        // get the space... allocate and set up the reference count before it
        RefCountedObject<ObjType>* refCountedObject = fromMe.getTarget();
        ObjType* object = refCountedObject->getObject();
#ifdef DEBUG_OBJECT_MODEL
        void* space =
            getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE +
                                      typeInfo.getSizeOfConstituentObject(
                                          /*fromMe.getTarget ()->getObject ()*/ (void*)object),
                                  typeInfo.getTypeCode());
#else
        void* space =
            getAllocator().getRAM(REF_COUNT_PREAMBLE_SIZE +
                                  typeInfo.getSizeOfConstituentObject(
                                      /*fromMe.getTarget ()->getObject ()*/ (void*)object));
#endif
        // see if there was not enough RAM
        if (space == nullptr) {
            std::cout << "Not enough memory when doing a deep copy with TypeId="
                      << typeInfo.getTypeCode() << std::endl;
            DEC_OLD_REF_COUNT;
            offset = -1;
            throw myException;
        }

        offset = CHAR_PTR(space) - CHAR_PTR(this);

        // set the reference count to one then decrement the old ref count
        getTarget()->setRefCount(1);

        // copy over the object; use a virtual method so that we get everything set up and copied
        // correctly
        try {
#ifdef DEBUG_DEEP_COPY
            int typeId = typeInfo.getTypeCode();
            if ((typeId == 0) || (typeId > 126)) {
                PDB_COUT << "Handle operator=: typeInfo=" << typeId
                         << " before setUpAndCopyFromConstituentObject" << std::endl;
            }
#endif
            typeInfo.setUpAndCopyFromConstituentObject(getTarget()->getObject(),
                                                       fromMe.getTarget()->getObject());
        } catch (NotEnoughSpace& n) {
            PDB_COUT << "Not enough memory when doing a deep copy with TypeId="
                     << typeInfo.getTypeCode() << std::endl;
            getTarget()->decRefCount(typeInfo);
            DEC_OLD_REF_COUNT;
            offset = -1;
            throw n;
        }

        DEC_OLD_REF_COUNT;

        // finally, do the assignment
        return *this;

        // if the RHS is not in the current allocator or the LHS handle is not in the
        // current allocator, then we just do a shallow copy
    } else {
#ifdef DEBUG_OBJECT_MODEL
        if (!getAllocator().contains(fromMe.getTarget()) && !getAllocator().contains(this)) {
            std::cout << "#################################################" << std::endl;
            std::cout << "Both LHS and RHS are not in current block" << std::endl;
            std::cout << "RHS typeinfo =" << typeInfo.getTypeCode() << std::endl;
            std::cout << "#################################################" << std::endl;
        }
#endif
        // set the offset
        offset = CHAR_PTR(fromMe.getTarget()) - CHAR_PTR(this);

        // increment the reference count then decrement the old ref count...
        // it is important to do it in this order to correctly handle self-
        // assignments without accidentally setting the count to zero
        getTarget()->incRefCount();
        DEC_OLD_REF_COUNT;

        return *this;
    }
}

template <class ObjType>
template <class ObjTypeTwo>
Handle<ObjType>& Handle<ObjType>::operator=(const Handle<ObjTypeTwo>& fromMe) {

    ObjType* one = nullptr;
    ObjTypeTwo* two = nullptr;
    convert(one, two);  // this line will not compile with a bad assignment

    // get the thing that we used to point to
    GET_OLD_TARGET;

    // if we got a null pointer as the RHS, then we are the RHS
    if (fromMe == nullptr) {
        DEC_OLD_REF_COUNT;
        offset = -1;
        return *this;
    }

    typeInfo = fromMe.typeInfo;
    if (typeInfo.getTypeCode() == 0) {
        PDB_COUT << "Handle operator = ObjTypeTwo: typeInfo = 0 before getSizeConstituentObject"
                 << std::endl;
    }
    // if the RHS is not in the current allocator, but the handle is, then
    // we need to copy it over using a deep copy
    if (!getAllocator().contains(fromMe.getTarget()) && getAllocator().contains(this)) {

// get the space... allocate and set up the reference count before it
#ifdef DEBUG_OBJECT_MODEL
        void* space = getAllocator().getRAM(
            REF_COUNT_PREAMBLE_SIZE +
                typeInfo.getSizeOfConstituentObject(fromMe.getTarget()->getObject()),
            typeInfo.getTypeCode());
#else
        void* space = getAllocator().getRAM(
            REF_COUNT_PREAMBLE_SIZE +
            typeInfo.getSizeOfConstituentObject(fromMe.getTarget()->getObject()));
#endif
        // see if there was not enough RAM
        if (space == nullptr) {
            std::cout << "Not enough memory when doing a deep copy with TypeId="
                      << typeInfo.getTypeCode() << std::endl;
            DEC_OLD_REF_COUNT;
            offset = -1;
            throw myException;
        }

        offset = CHAR_PTR(space) - CHAR_PTR(this);

        // set the reference count to one then decrement the old ref count
        getTarget()->setRefCount(1);

        // copy over the object; use a virtual method so that we get everything set up and copied
        // correctly
        try {
            typeInfo.setUpAndCopyFromConstituentObject(getTarget()->getObject(),
                                                       fromMe.getTarget()->getObject());
        } catch (NotEnoughSpace& n) {
            PDB_COUT << "Not enough memory when doing a deep copy with TypeId="
                     << typeInfo.getTypeCode() << std::endl;
            getTarget()->decRefCount(typeInfo);
            DEC_OLD_REF_COUNT;
            offset = -1;
            throw n;
        }

        DEC_OLD_REF_COUNT;

        // finally, do the assignment
        return *this;

        // if the RHS is not in the current allocator or the LHS handle is not in the
        // current allocator, then we just do a shallow copy
    } else {
#ifdef DEBUG_OBJECT_MODEL
        if (!getAllocator().contains(fromMe.getTarget()) && !getAllocator().contains(this)) {
            std::cout << "#################################################" << std::endl;
            std::cout << "Both LHS and RHS are not in current block" << std::endl;
            std::cout << "RHS typeinfo =" << typeInfo.getTypeCode() << std::endl;
            std::cout << "#################################################" << std::endl;
        }
#endif
        // set the offset
        offset = CHAR_PTR(fromMe.getTarget()) - CHAR_PTR(this);

        // increment the reference count then decrement the old ref count...
        // it is important to do it in this order to correctly handle self-
        // assignments without accidentally setting the count to zero
        getTarget()->incRefCount();
        DEC_OLD_REF_COUNT;

        return *this;
    }
}

// de-reference operators
template <class ObjType>
ObjType* Handle<ObjType>::operator->() const {

    // fix the vTable pointer and return a reference
    // std :: cout << "to setVTablePtr in handle -> operator" << std :: endl;
    typeInfo.setVTablePtr(getTarget()->getObject());
    // std :: cout << "setVTablePtr done" << std :: endl;
    return getTarget()->getObject();
}

template <class ObjType>
void Handle<ObjType>::setOffset(int64_t toMe) {
    offset = toMe;
}

template <class ObjType>
int64_t Handle<ObjType>::getOffset() const {
    return offset;
}

template <class ObjType>
ObjType& Handle<ObjType>::operator*() const {

    // fix the vTable pointer and return a reference
    typeInfo.setVTablePtr(getTarget()->getObject());
    return *(getTarget()->getObject());
}


// gets a pointer to the target object
template <class ObjType>
RefCountedObject<ObjType>* Handle<ObjType>::getTarget() const {
    return (RefCountedObject<ObjType>*)(CHAR_PTR(this) + offset);
}


// JiaNote: to shallow copy a handle to current allocation block
// This is to improve the performance of current pipeline bundling
template <class ObjType>
Handle<ObjType>& Handle<ObjType>::shallowCopyToCurrentAllocationBlock(
    const Handle<ObjType>& copyMe) {

    // get the thing that we used to point to
    GET_OLD_TARGET;

    // if we got a null pointer as the RHS, then we are the RHS
    if (copyMe == nullptr) {
        DEC_OLD_REF_COUNT;
        offset = -1;
        return *this;
    }

    typeInfo = copyMe.typeInfo;

    // set the offset
    offset = CHAR_PTR(copyMe.getTarget()) - CHAR_PTR(this);

    // increment the reference count then decrement the old ref count...
    // it is important to do it in this order to correctly handle self-
    // assignments without accidentally setting the count to zero
    getTarget()->incRefCount();
    DEC_OLD_REF_COUNT;

    return *this;
}
}

#endif
