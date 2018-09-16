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
 * CatalogUserTypeMetadata.h
 *
 */

#ifndef CATALOG_USER_TYPE_METADATA_H_
#define CATALOG_USER_TYPE_METADATA_H_

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include "PDBString.h"
#include "PDBVector.h"

//  PRELOAD %CatalogUserTypeMetadata%

using namespace std;

namespace pdb {

/**
 *   CatalogUserTypeMetadata encapsulates information for a given user-defined object
 *   including:
 *
 *   1) typeID: numeric identifier assigned automatically by the catalog
 *   2) name: the name of the type
 *   2) typeCategory either {built-in, user-defined}
 *   3) soBytes: are the bytes of the .so library
 */

class CatalogUserTypeMetadata : public pdb::Object {

public:
  ENABLE_DEEP_COPY

  CatalogUserTypeMetadata() {
    this->typeID = -1;
    this->soBytes = nullptr;
  };

  ~CatalogUserTypeMetadata() = default;

  CatalogUserTypeMetadata(int32_t typeID,
                          const std::string &name,
                          const std::string &typeCategory,
                          const char *libraryBytes,
                          size_t librarySize) {

    // set the type info
    this->typeID = typeID;
    this->typeCategory = typeCategory;
    this->typeName = name;
    this->soBytes = pdb::makeObject<pdb::Vector<char>>(librarySize, librarySize);

    // copy the bytes
    memcpy(this->soBytes->c_ptr(), libraryBytes, librarySize);
  }

  CatalogUserTypeMetadata(const CatalogUserTypeMetadata &pdbCatalogEntryToCopy) {

    // copy the type info
    this->typeID = pdbCatalogEntryToCopy.typeID;
    this->typeCategory = pdbCatalogEntryToCopy.typeCategory;
    this->typeName = pdbCatalogEntryToCopy.typeName;
    this->soBytes = pdb::makeObject<pdb::Vector<char>>(soBytes->size(), soBytes->size());

    // copy the bytes
    memcpy(this->soBytes->c_ptr(), soBytes->c_ptr(), soBytes->size());
  }

  // numeric unique ID for a user-defined object starts from 8192
  int32_t typeID;
  // the name of the type
  pdb::String typeName;
  // either {built-in, user-defined}
  pdb::String typeCategory;
  // the bytes containing the .so library file
  pdb::Handle<pdb::Vector<char>> soBytes;
};

} /* namespace pdb */

#endif /* CATALOG_USER_TYPE_METADATA_H_ */
