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
#ifndef PDB_TCAPPARSER_TRANSLATIONUNIT_H
#define PDB_TCAPPARSER_TRANSLATIONUNIT_H

#include <memory>
#include <vector>

#include "TcapStatement.h"

using std::make_shared;
using std::shared_ptr;
using std::vector;

namespace pdb_detail
{
    class TranslationUnit
    {

    public:

        shared_ptr<vector<shared_ptr<TcapStatement>>> const statements = make_shared<vector<shared_ptr<TcapStatement>>>();
    };

}

#endif //PDB_TCAPPARSER_TRANSLATIONUNIT_H
