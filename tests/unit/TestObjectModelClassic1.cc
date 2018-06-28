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

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <ctime>
#include <iterator>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <chrono>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <vector>

class Employee {
    std::string* name;
    int age;

public:
    ~Employee() {
        delete name;
    }

    Employee() {}

    Employee(std::string nameIn, int ageIn) {
        name = new std::string(nameIn);
        age = ageIn;
    }
};

class Supervisor {

private:
    Employee* me;
    std::vector<Employee*> myGuys;

public:
    ~Supervisor() {
        delete me;
        for (auto a : myGuys) {
            delete a;
        }
    }

    Supervisor() {}

    Supervisor(std::string name, int age) {
        me = new Employee(name, age);
    }

    void addEmp(Employee* addMe) {
        myGuys.push_back(addMe);
    }
};

int main(int argc, char* argv[]) {

    //parse the parameters
    //parameter 1: number of objects

    if (argc <= 1) {
        std::cout << "Usage: #numObjects" << std::endl;
    }

    int numObjects = atoi(argv[1]);

    // for timing
    auto begin = std::chrono::high_resolution_clock::now();

    std::vector<Supervisor*> supers;

    // put a lot of copies of it into a vector
    for (int i = 0; true; i++) {

        supers.push_back(new Supervisor("Joe Johnson"+std::to_string(i), 20 + (i % 29)));
        for (int j = 0; j < 10; j++) {
            Employee* temp = new Employee("Steve Stevens"+std::to_string(i)+std::to_string(j), 20 + ((i + j) % 29));
            supers[supers.size() - 1]->addEmp(temp);
        }

        if (i > numObjects) {
            break;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to create all of the objects: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;

    for (auto v : supers)
        delete v;
}
