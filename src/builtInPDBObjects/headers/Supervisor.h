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

#ifndef SUPERVISOR_H
#define SUPERVISOR_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"
#include "Employee.h"
#include "ExportableObject.h"

//  PRELOAD %Supervisor%

namespace pdb {

class Supervisor : public ExportableObject {

public:

        Handle <Employee> me;
        Vector <Handle <Employee>> myGuys;

	ENABLE_DEEP_COPY

        ~Supervisor () {}
        Supervisor () {}

	Supervisor (std :: string name, int age) {
                me = makeObject <Employee> (name, age);
	}

        Supervisor (std :: string name, int age, std :: string department, double salary) {
                me = makeObject <Employee> (name, age, department, salary);
        }

        Handle <Employee> &getEmp (int who) {
                return myGuys[who];
        }

	int getNumEmployees () {
		return myGuys.size ();
	}
       
        String getDepartment() {
                return me->department;
        }

        Handle<String> getName() {
                return me->getName();
        }

        void addEmp (Handle <Employee> &addMe) {
                myGuys.push_back (addMe);
        }

	Handle <Employee> getSteve () {
		for (int i = 0; i < myGuys.size (); i++) {
			if (myGuys[i]->getName () == "Steve Stevens")
				return myGuys[i];
		}
		return nullptr;
	}

	Handle <Employee> &getMe () {
		return me;
	}

        void print () final {
                me->print ();
                std :: cout << "\nPlus have " << myGuys.size () << " employees.\n";
		/*if (myGuys.size () > 0) {
			std :: cout << "\t (One is ";
			myGuys[0]->print ();
			std :: cout << ")\n";
		}*/
                for (int i = 0; i < myGuys.size(); i++) {
                        std :: cout << "emp " << i << ": ";
                        myGuys[i]->print();
			std :: cout << " ";
                }

        }


        std :: string toSchemaString ( std :: string format ) override {
                return "";
        }

        std :: string toValueString ( std :: string format ) override {
                if (format == "json") {
                    char buffer[1024];
                    sprintf(buffer, "{\"name\":\"%s\",\"age\":%d,\"salary\":%f,\"department\":\"%s\",\"employees\":[", me->getName()->c_str(), me->getAge(), me->getSalary(), me->department.c_str());
                    std :: string ret = std :: string (buffer);
                    if (myGuys.size() > 0) { 
                        char buffer[1024];
                        sprintf(buffer, "{\"name\":\"%s\",\"age\":%d,\"salary\":%f,\"department\":\"%s\"}", myGuys[0]->getName()->c_str(), myGuys[0]->getAge(), myGuys[0]->getSalary(), myGuys[0]->department.c_str());
                        ret += std :: string (buffer);
                    }
                    for (int i = 1; i < myGuys.size(); i++) {
                        char buffer[1024];
                        sprintf(buffer, ",{\"name\":\"%s\",\"age\":%d,\"salary\":%f,\"department\":\"%s\"}", myGuys[i]->getName()->c_str(), myGuys[i]->getAge(), myGuys[i]->getSalary(), myGuys[i]->department.c_str());
                        ret += std :: string (buffer);
                    }
                    ret += std :: string ("]}\n");
                    return ret;
                } else {
                    return "";
                }
        }


        std :: vector < std :: string > getSupportedFormats () override {
               std :: vector < std :: string> ret;
               ret.push_back("json");
               return ret;
        }

};

}

#endif
