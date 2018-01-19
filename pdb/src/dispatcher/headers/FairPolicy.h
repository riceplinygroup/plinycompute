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
// Created by Joseph Hwang on 9/13/16.
//

#ifndef OBJECTQUERYMODEL_FAIRPOLICY_H
#define OBJECTQUERYMODEL_FAIRPOLICY_H

/**
 * RandomPolicy simply selects a single storage node from its Storage Nodes List to send the entire
 * Vector of data to.
 * We send the entire Vector to the storage node that has received the least data for the given set
 * from this Dispatcher
 * thus far
 */


#endif  // OBJECTQUERYMODEL_FAIRPOLICY_H
