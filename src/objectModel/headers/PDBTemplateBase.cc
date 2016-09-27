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

#ifndef TEMPLATE_BASE_CC
#define TEMPLATE_BASE_CC

namespace pdb {

inline PDBTemplateBase :: PDBTemplateBase () {
		info = 0;
}

inline int16_t PDBTemplateBase :: getTypeCode () const {
	if (info > 0)
		return (int16_t) info;
	else
		return -1;
}

inline PDBTemplateBase &PDBTemplateBase :: operator = (const PDBTemplateBase &toMe) {
	info = toMe.info;
	return *this;
}

// set up the type
template <class ObjType>
void PDBTemplateBase :: setup () {

	// if we descend from Object, then get the type code
	if (std::is_base_of <Object, ObjType>::value) {
		info = (int16_t) getTypeID <ObjType> (); 
	} else  {

		// we could not recognize this type, so just record the size
		info = - ((int32_t) sizeof (ObjType));
	}
}

// this deletes an object of type ObjType
inline void PDBTemplateBase :: deleteConstituentObject (void *deleteMe) const {

	// if we are not derived from Object, do nothing
	if (info > 0) {
		// we are going to install the vTable pointer for an object of type ObjType into temp
		void *temp = nullptr;
		((Object *) &temp)->setVTablePtr (VTableMap :: getVTablePtr ((int16_t) info));

		// now call the deleter for that object type
		if (temp != nullptr) {
			((Object *) &temp)->deleteObject (deleteMe);

		// in the worst case, we could not fix the vTable pointer, so try to use the object's vTable pointer
		} else {
			((Object *) deleteMe)->deleteObject (deleteMe);
		}
	}
}

inline void PDBTemplateBase :: setUpAndCopyFromConstituentObject (void *target, void *source) const {

	// if we are derived from Object, use the virtual function
	if (info > 0) {

		// we are going to install the vTable pointer for an object of type ObjType into temp
		void *temp = nullptr;
		((Object *) &temp)->setVTablePtr (VTableMap :: getVTablePtr ((int16_t) info));

		// now call the construct-and-copy operation for that object type
		if (temp != nullptr) {
			((Object *) &temp)->setUpAndCopyFrom (target, source);

		// in the worst case, we could not fix the vTable pointer, so try to use the object's vTable pointer
		} else {
			((Object *) source)->setUpAndCopyFrom (target, source);
		}

	} else {
		
		// just do a memmove
		memmove (target, source, -info);
	}
}	

inline size_t PDBTemplateBase :: getSizeOfConstituentObject (void *ofMe) const {

	// if we are derived from Object, use the virtual function
	if (info > 0) {
		// we are going to install the vTable pointer for an object of type ObjType into temp
		void *temp = nullptr;
		((Object *) &temp)->setVTablePtr (VTableMap :: getVTablePtr ((int16_t) info));

		// now get the size
		if (temp != nullptr) {
			return ((Object *) &temp)->getSize (ofMe);

		// in the worst case, we could not fix the vTable pointer, so try to use the object's vTable pointer
		} else {
			return ((Object *) ofMe)->getSize (ofMe);
		}
	} else {
		return -info;
	}
}

inline void PDBTemplateBase :: setVTablePtr (void *forMe) const {
	((Object *) forMe)->setVTablePtr (VTableMap :: getVTablePtr ((int16_t) info));		
}

inline bool PDBTemplateBase :: descendsFromObject () const {
	return info > 0;
}

}
#endif
