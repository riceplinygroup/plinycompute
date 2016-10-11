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
 * CatalogPermissionsMetadata.h
 *
 *  Created on: Sept 12, 2016
 *      Author: carlos
 */

#ifndef CATALOG_PERMISSIONS_METADATA_H_
#define CATALOG_PERMISSIONS_METADATA_H_

#include <iostream>
#include "Object.h"
#include "PDBString.h"

//  PRELOAD %CatalogPermissionsMetadata%

using namespace std;

namespace pdb {

// This class serves to store information about users' permissions to databases in an
// instance of PDB.
// It also provides methods for maintaining their associated metadata.
// Clients of this class will access this information using a handler to the catalog.

    class CatalogPermissionsMetadata : public Object {
    public:

        ENABLE_DEEP_COPY

        //TODO: methods for adding, revoking permissions, etc.
        CatalogPermissionsMetadata() {
            // TODO Auto-generated constructor stub

        }

        ~CatalogPermissionsMetadata() {
            // TODO Auto-generated destructor stub
        }
    };

} /* namespace pdb */

//#include "CatalogPermissionsMetadata.cc"

#endif /* CATALOG_PERMISSIONS_METADATA_H_ */
