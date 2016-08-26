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

// this contains the GET_V_TABLE macro, which is used to create a shared library
// that can send an object type around the compute cluster

#ifndef GET_V_TABLE 
#define GET_V_TABLE(TYPE_NAME) 							\
										\
namespace pdb {									\
										\
/* this returns the name of the type that we are loading */			\
char typeName[300];								\
    										\
extern "C" {									\
										\
char *getObjectTypeName () {							\
	std :: string typeString = getTypeName <TYPE_NAME> ();			\
	memmove (typeName, typeString.c_str (), typeString.size () + 1);	\
	return typeName;							\
}										\
										\
/* this returns an instance of the vtable of the type that we are loading */	\
void *getObjectVTable () {							\
	void *returnVal = nullptr;						\
        try {									\
		Handle <TYPE_NAME> temp = makeObjectOnTempAllocatorBlock 	\
			<TYPE_NAME> (1024 * 1024 * 4);					\
		returnVal = temp->getVTablePtr ();				\
        } catch (NotEnoughSpace &e) {						\
                std :: cout << "Not enough memory to allocate TYPE_NAME";	\
		std :: cout << " to extract the vTable.\n";			\
        }									\
	return returnVal;							\
}										\
										\
}										\
										\
}										\

#endif
