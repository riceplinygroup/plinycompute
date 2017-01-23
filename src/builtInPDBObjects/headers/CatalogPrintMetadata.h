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
 * CatalogPrintMetadata.h
 *
 *  Created on: Sept 12, 2016
 *      Author: carlos
 */

#ifndef CATALOG_PRINT_METADATA_H_
#define CATALOG_PRINT_METADATA_H_

#include <iostream>
#include "Object.h"
#include "PDBString.h"
#include "PDBVector.h"

//  PRELOAD %CatalogPrintMetadata%

using namespace std;

namespace pdb {

// This class serves to retrieve metadata from the Catalog and returns it
// as a String

    class CatalogPrintMetadata : public Object {
    public:

        CatalogPrintMetadata() {
        }

        CatalogPrintMetadata(string itemName) :
        itemName(itemName)
        {
        }

        string getItemName(){
            return itemName;
        }

        ENABLE_DEEP_COPY

    private:
        string itemName;

    };

} /* namespace pdb */

#endif /* CATALOG_PRINT_METADATA_H_ */
