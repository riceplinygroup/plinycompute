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
#ifndef PDB_LOGICALPLANTESTS_BUILDLOGICALPLANTESTS_H
#define PDB_LOGICALPLANTESTS_BUILDLOGICALPLANTESTS_H

#include "qunit.h"

using QUnit::UnitTest;

namespace pdb_tests
{
    void testBuildLoad(UnitTest &qunit);

    void testBuildApplyFunction(UnitTest &qunit);

    void testBuildApplyMethod1(UnitTest &qunit);

    void testBuildApplyMethod2(UnitTest &qunit);

    void testBuildApplyMethod3(UnitTest &qunit);

    void testBuildApplyFilter(UnitTest &qunit);

    void testBuildStore(UnitTest &qunit);
}

#endif //PDB_LOGICALPLANTESTS_BUILDLOGICALPLANTESTS_H
