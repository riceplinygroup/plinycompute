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
 * File:   PDBAlarm.h
 * Author: Chris
 *
 * Created on September 25, 2015, 5:18 PM
 */

#ifndef PDBALARM_H
#define PDBALARM_H

enum PDBAlarm {
    WorkAllDone,
    GenericError,
    FatalServerError,
    ServerError,
    OpenFileError,
    BuildFeatureError,
    QueryError,
    HeartBeatError

};

#endif /* PDBALARM_H */
