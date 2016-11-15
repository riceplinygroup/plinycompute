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
#include "StoreOperation.h"

using std::make_shared;

namespace pdb_detail
{

    StoreOperation::StoreOperation(TcapIdentifier outputTable,
                                   shared_ptr<vector<TcapIdentifier>> columnsToStore, string destination)
            : StoreOperation(make_shared<vector<TcapAttribute>>(), outputTable, columnsToStore, destination)
    {

    }

    StoreOperation::StoreOperation(shared_ptr<vector<TcapAttribute>> attributes, TcapIdentifier outputTable,
                                   shared_ptr<vector<TcapIdentifier>> columnsToStore, string destination)
            : TcapStatement(attributes), outputTable(outputTable), columnsToStore(columnsToStore), destination(destination)
    {

    }

    void StoreOperation::match(function<void(TableAssignment&)>, function<void(StoreOperation&)> forStore)
    {
        forStore(*this);
    }
}