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

#ifndef PDBCATALOG_VTABLEMAP_CONST_C_
#define PDBCATALOG_VTABLEMAP_CONST_C_

/*
 * VTableMap.cc
 *
 *  Created on: Dec 11, 2015
 *      Author: carlos
 */

// This header is auto-generated by SConstruct... it includes all of the header files
// in BuiltInPDBObjects/headers

#include "BuiltinPDBObjects.h"
#include "VTableMap.cc"

#include <dlfcn.h>
#include <unistd.h>
#include <cctype>

namespace pdb {

	VTableMap :: VTableMap() {
		logger = nullptr;
		parent = nullptr;
		pthread_mutex_init(&(myLock), nullptr);
        	for (int i = 0; i < 16384; i++) {
			allVTables.push_back (nullptr);
		}

		// this will add a type identifier for each one of the built-in types
		// (building the map from type name to ID) and then after that, it
		// is going to create an instance of each of the types, and extract
		// the v-table pointer from it, adding that to the allVTables vector
		#include "BuiltinPDBObjects.cc"

	}

	VTableMap :: ~VTableMap () {
		pthread_mutex_destroy(&myLock);
	}

	// various global variables
	Allocator allocator;
	VTableMap globalVTable;
	VTableMap *theVTable = &globalVTable;
	NotEnoughSpace myException;

} /* namespace pdb */


#endif
