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
//
// Created by Joseph Hwang on 9/12/16.
//

#ifndef OBJECTQUERYMODEL_DISPATCHERADDDATA_H
#define OBJECTQUERYMODEL_DISPATCHERADDDATA_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

// PRELOAD %DispatcherAddData%

namespace pdb {

// encapsulates a request to add data to a set in storage
    class DispatcherAddData  : public Object {

    public:

        DispatcherAddData () {}
        ~DispatcherAddData () {}

        DispatcherAddData (std :: string databaseName, std :: string setName, std :: string typeName) : databaseName (databaseName), setName (setName),
                                                                                                 typeName (typeName) {}

        std :: string getDatabaseName () {
            return databaseName;
        }

        std :: string getSetName () {
            return setName;
        }

        std :: string getTypeName () {
            return typeName;
        }

        ENABLE_DEEP_COPY

    private:

        String databaseName;
        String setName;
        String typeName;

    };

}

#endif //OBJECTQUERYMODEL_DISPATCHERADDDATA_H
