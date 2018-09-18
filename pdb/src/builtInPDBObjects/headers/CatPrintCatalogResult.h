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

#ifndef CATALOG_PRINT_CATALOG_RESULT_H_
#define CATALOG_PRINT_CATALOG_RESULT_H_

#include <iostream>
#include "Object.h"
#include "PDBString.h"
#include "PDBVector.h"

//  PRELOAD %CatPrintCatalogResult%

using namespace std;

namespace pdb {

/**
 * This class serves to return the output of a CatPrintCatalogRequest
 */
class CatPrintCatalogResult : public Object {
 public:

  CatPrintCatalogResult() = default;

  explicit CatPrintCatalogResult(const std::string &output) : output(output) {}

  // Copy constructor
  CatPrintCatalogResult(const CatPrintCatalogResult &pdbItemToCopy) {
    output = pdbItemToCopy.output;
  }

  // Copy constructor
  explicit CatPrintCatalogResult(const Handle<CatPrintCatalogResult> &pdbItemToCopy) {
    output = pdbItemToCopy->output;
  }

  ENABLE_DEEP_COPY

  // the output
  String output;
};

} /* namespace pdb */

#endif /* CATALOG_PRINT_METADATA_H_ */