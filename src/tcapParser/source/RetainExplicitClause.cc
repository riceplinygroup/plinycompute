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
#include "RetainExplicitClause.h"

#include <vector>

using std::make_shared;
using std::vector;

namespace pdb_detail
{
    RetainExplicitClause::RetainExplicitClause(shared_ptr<vector<TcapIdentifier>> columns) : columns(columns)
    {
    }

    RetainExplicitClause::RetainExplicitClause(TcapIdentifier column)
    {
        columns = make_shared<vector<TcapIdentifier>>();
        columns->push_back(column);
    }

    bool RetainExplicitClause::isAll()
    {
        return false;
    }

    bool RetainExplicitClause::isNone()
    {
        return false;
    }

    void RetainExplicitClause::match(function<void(RetainAllClause &)>,
                                     function<void(RetainExplicitClause &)> forExplicit,
                                     function<void(RetainNoneClause &)>)
    {
        forExplicit(*this);
    }

}
