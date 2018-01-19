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
 * CatalogCloseSQLiteDBHandler.h
 *
 */

#ifndef SRC_BUILTINPDBOBJECTS_HEADERS_CATALOGCLOSESQLITEDBHANDLER_H_
#define SRC_BUILTINPDBOBJECTS_HEADERS_CATALOGCLOSESQLITEDBHANDLER_H_

#include "Object.h"

//  PRELOAD %CatalogCloseSQLiteDBHandler%

using namespace std;

namespace pdb {

// Encapsulates a request for closing the SQLite DB Handler
class CatalogCloseSQLiteDBHandler : public Object {
public:
    CatalogCloseSQLiteDBHandler() {}

    ENABLE_DEEP_COPY
};

} /* namespace pdb */

#endif /* SRC_BUILTINPDBOBJECTS_HEADERS_CATALOGCLOSESQLITEDBHANDLER_H_ */
