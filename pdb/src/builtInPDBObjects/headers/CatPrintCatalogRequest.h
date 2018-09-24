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
 * CatPrintCatalogRequest.h
 *
 */

#ifndef CATALOG_PRINT_CATALOG_REQUEST_H_
#define CATALOG_PRINT_CATALOG_REQUEST_H_

#include <iostream>
#include "Object.h"
#include "PDBString.h"
#include "PDBVector.h"

//  PRELOAD %CatPrintCatalogRequest%

using namespace std;

namespace pdb {

/**
 * This class serves to retrieve metadata from the Catalog and returns it as a String
 */
class CatPrintCatalogRequest : public Object {
public:

    CatPrintCatalogRequest() = default;

    CatPrintCatalogRequest(const std::string &itemName, const std::string &categoryIn) : itemName(itemName), category(categoryIn) {}

    // Copy constructor
    CatPrintCatalogRequest(const CatPrintCatalogRequest& pdbItemToCopy) {
        itemName = pdbItemToCopy.itemName;
        category = pdbItemToCopy.category;
    }

    // Copy constructor
    explicit CatPrintCatalogRequest(const Handle<CatPrintCatalogRequest>& pdbItemToCopy) {
        itemName = pdbItemToCopy->itemName;
        category = pdbItemToCopy->category;
    }

    ENABLE_DEEP_COPY

    /**
     * the category to print (databases, sets, nodes, udts) udts are user-defined types
     */
    String category;

    /**
     * the item Name to print at this point it just serves to print he catalog in a database
     */
    String itemName;
};

} /* namespace pdb */

#endif /* CATALOG_PRINT_METADATA_H_ */
