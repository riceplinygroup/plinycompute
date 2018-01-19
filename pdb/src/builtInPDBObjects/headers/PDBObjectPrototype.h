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

#ifndef PDBOBJECT_PROTOTYPE_H
#define PDBOBJECT_PROTOTYPE_H

// What's the idea here?
//
// Well, we need to get each pdb template type into the v-table map.  Each template type will then
// be assigned a type ID that every instance of the template will share, no matter what the type
// parameterization.  The PDBTemplateBase within the template will be used to remember the exact
// type of the object inside of the template, so that we can perform operations on it at a later
// time.

//  PRELOAD %Object%

#endif
