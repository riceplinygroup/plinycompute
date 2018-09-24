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

#ifndef CAT_REG_TYPE_H
#define CAT_REG_TYPE_H

#include "Object.h"
#include "Handle.h"
#include "PDBVector.h"

// PRELOAD %CatRegisterType%

namespace pdb {

/**
 * Encapsulates a request to register a type in the catalog
 */
class CatRegisterType : public Object {

public:

  /**
   * The default constructor
   */
  CatRegisterType() = default;

  /**
   * Initializes this request with the bytes of the .so library
   * @param bytes - the bytes of the .so library
   * @param fileLength - the size of the .so library
   */
  CatRegisterType(char* bytes, size_t fileLength) {

    // allocate the vector
    libraryBytes = makeObject<Vector<char>>(fileLength, fileLength);

    // copy the bytes
    memcpy(libraryBytes->c_ptr(), bytes, fileLength);
  }

  /**
   * Default destructor
   */
  ~CatRegisterType() = default;

  /**
   * Returns the bytes of the .so library
   * @return the bytes
   */
  char *getLibraryBytes() {
    return libraryBytes->c_ptr();
  }

  /**
   * Returns the .so library size
   * @return the size
   */
  size_t getLibrarySize() {
    return libraryBytes->size();
  }

  ENABLE_DEEP_COPY

private:

  /**
   * Contains the bytes of the library
   */
  Handle<Vector<char>> libraryBytes;
};

}

#endif
