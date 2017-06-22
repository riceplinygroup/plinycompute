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

#ifndef SCAN_LDA_DOCUMENT_SET_H
#define SCAN_LDA_DOCUMENT_SET_H

#include "ScanUserSet.h"
#include "LDADocument.h"

using namespace pdb;
class ScanLDADocumentSet : public ScanUserSet <LDADocument> {

public:
	ENABLE_DEEP_COPY

        ScanLDADocumentSet () {
        }

        ScanLDADocumentSet (std :: string dbName, std :: string setName) {
            setDatabaseName( dbName );
            setSetName ( setName );
        }

};


#endif
