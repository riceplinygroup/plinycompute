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

#ifndef SIMPLE_EMPLOYEE_H
#define SIMPLE_EMPLOYEE_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"
#include "ExportableObject.h"
#include <vector>

class SimpleEmployee : public ExportableObject {

public:
        pdb :: String name;
        int age;
        double salary;
        pdb :: String department; 

	ENABLE_DEEP_COPY

        ~SimpleEmployee () { }
        SimpleEmployee () {}

        void print() override {
            std :: cout << name << "," << age << "," << salary << "," << department << std :: endl;
        }


        std :: string toSchemaString ( std :: string format ) override {
                if (format == "csv") {
                    return "name,age,salary,department\n";
                } else {
                    return "";
                }
        }

        std :: string toValueString ( std :: string format ) override {
                if (format == "csv") {
                    char buffer[65535];
                    sprintf(buffer, "%s,%d,%f,%s\n", name.c_str(), age, salary, department.c_str());
                    return buffer;
                } else {
                    return "";
                }
        }


        std :: vector < std :: string > getSupportedFormats () override {
               std :: vector < std :: string> ret;
               ret.push_back("csv");
               return ret;
        }


        SimpleEmployee (std :: string nameIn, int ageIn, double salaryIn, std :: string departmentIn) {
                name = nameIn;
                age = ageIn;
                salary = salaryIn;
                department = departmentIn;
        }

};


#endif
