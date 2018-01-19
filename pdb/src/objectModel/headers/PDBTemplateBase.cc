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

#include "Object.h"
#include "VTableMap.h"
#include "PDBString.h"
#include "Handle.h"
#include "BuiltInObjectTypeIDs.h"

#ifndef TEMPLATE_BASE_CC
#define TEMPLATE_BASE_CC

namespace pdb {

inline PDBTemplateBase::PDBTemplateBase() {
    info = 0;
}

inline int16_t PDBTemplateBase::getTypeCode() const {
    if (info > 0)
        return (int16_t)info;
    else
        return -1;
}

inline int32_t PDBTemplateBase::getExactTypeInfoValue() const {
    return info;
}


inline PDBTemplateBase& PDBTemplateBase::operator=(const PDBTemplateBase& toMe) {
    info = toMe.info;
    return *this;
}

// set up the type
inline void PDBTemplateBase::set(int32_t toMe) {
    info = toMe;
}

// set up the type
template <class ObjType>
void PDBTemplateBase::setup() {

    // if we descend from Object, then get the type code
    if (std::is_base_of<Object, ObjType>::value) {
        info = (int16_t)getTypeID<ObjType>();
        // std :: cout << "I'm a pdb Object with info="<< info << std :: endl;
    } else if (std::is_base_of<String, ObjType>::value) {
        info = String_TYPEID;
        // std :: cout << "I am a String object with info=" << info << std :: endl;
    } else if (std::is_base_of<HandleBase, ObjType>::value) {
        info = Handle_TYPEID;
        // std :: cout << "I am a Handle object with info=" << info << std :: endl;
    } else {
        // we could not recognize this type, so just record the size
        info = -((int32_t)sizeof(ObjType));
        // std :: cout << "I am a C++ object with info=" << info << std :: endl;
    }
}

// this deletes an object of type ObjType
inline void PDBTemplateBase::deleteConstituentObject(void* deleteMe) const {

    // if we are a string
    if (info == String_TYPEID) {
        ((String*)deleteMe)->~String();

        // if we are a Handle
    } else if (info == Handle_TYPEID) {
        ((Handle<Nothing>*)deleteMe)->~Handle();

        // else if we are not derived from Object, do nothing
    } else if (info > 0) {
        // we are going to install the vTable pointer for an object of type ObjType into temp
        void* temp = nullptr;
        // std :: cout << "to getVTablePtr for deleteConsitituentObject" << info << std :: endl;
        ((Object*)&temp)->setVTablePtr(VTableMap::getVTablePtr((int16_t)info));

        // now call the deleter for that object type
        if (temp != nullptr) {
            ((Object*)&temp)->deleteObject(deleteMe);

            // in the worst case, we could not fix the vTable pointer, so try to use the object's
            // vTable pointer
        } else {
            ((Object*)deleteMe)->deleteObject(deleteMe);
        }
    }
}

inline void PDBTemplateBase::setUpAndCopyFromConstituentObject(void* target, void* source) const {

    // if we are a string
    if (info == String_TYPEID) {
        new (target) String();
        *((String*)target) = *((String*)source);

        // if we are a Handle
    } else if (info == Handle_TYPEID) {
        new (target) Handle<Nothing>();
        *((Handle<Nothing>*)target) = *((Handle<Nothing>*)source);

        // if we are derived from Object, use the virtual function
    } else if (info > 0) {

        // we are going to install the vTable pointer for an object of type ObjType into temp
        void* temp = nullptr;
        // std :: cout << "to getVTablePtr for setUpAndCopyFromConsitituentObject" << info << std ::
        // endl;
        ((Object*)&temp)->setVTablePtr(VTableMap::getVTablePtr((int16_t)info));

        // now call the construct-and-copy operation for that object type
        if (temp != nullptr) {
            ((Object*)&temp)->setUpAndCopyFrom(target, source);

            // in the worst case, we could not fix the vTable pointer, so try to use the object's
            // vTable pointer
        } else {
            ((Object*)source)->setUpAndCopyFrom(target, source);
        }

    } else {

        // just do a memmove
        memmove(target, source, -info);
    }
}

inline size_t PDBTemplateBase::getSizeOfConstituentObject(void* ofMe) const {

    // if we are a string
    if (info == String_TYPEID) {
        return sizeof(String);

        // else if we are a handle
    } else if (info == Handle_TYPEID) {
        return sizeof(Handle<Nothing>);

        // if we are derived from Object, use the virtual function
    } else if (info > 0) {
        // we are going to install the vTable pointer for an object of type ObjType into temp
        void* temp = nullptr;
        ((Object*)&temp)->setVTablePtr(VTableMap::getVTablePtr((int16_t)info));

        // now get the size
        if (temp != nullptr) {
            return ((Object*)&temp)->getSize(ofMe);

            // in the worst case, we could not fix the vTable pointer, so try to use the object's
            // vTable pointer
        } else {
            return ((Object*)ofMe)->getSize(ofMe);
        }
    } else {
        return -info;
    }
}

inline void PDBTemplateBase::setVTablePtr(void* forMe) const {

    // if we are derived from Object, then we set the v table pointer; otherwise, no need
    if (info > Handle_TYPEID)
        ((Object*)forMe)->setVTablePtr(VTableMap::getVTablePtr((int16_t)info));
}

inline bool PDBTemplateBase::descendsFromObject() const {
    return info > 0;
}
}

#include "PDBString.cc"

#endif
