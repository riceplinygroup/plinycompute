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
#ifndef EXPORTABLE_OBJECT_H
#define EXPORTABLE_OBJECT_H

#include "Object.h"
#include <string>

class ExportableObject : public pdb :: Object {

public:

     //to simply print to std::cout
     virtual void print () = 0;
    
     //to return the schema string of this object: e.g. the csv header line for csv format
     //format can be "csv", "json", "parquet" and so on
     virtual std :: string toSchemaString(std :: string format) = 0;

     //to return the value string of this object: e.g. a line of comma separated values for csv format
     virtual std :: string toValueString(std :: string format) = 0;

     //one object may support multiple exporting formats
     //to return all supported formats
     virtual std :: vector<std :: string> getSupportedFormats() = 0;


};


#endif
