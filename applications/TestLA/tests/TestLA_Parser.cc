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
#include <iostream>

#include "LAParser.h"
#include "LAStatementsList.h"

// by Binhang, June 2017


int main(int argc, char** argv) {

    if (argc == 2) {
        FILE* targetCode = fopen(argv[1], "r");
        if (!targetCode) {
            std::cout << "No such file ! <" << argv[1] << ">" << std::endl;
            return -1;
        }

        LAscan_t myscanner;

        LAlex_init(&myscanner);

        LAset_in(targetCode, myscanner);

        std::cout << "Get started to parse the file!" << std::endl;

        LAStatementsList* myStatements = new LAStatementsList();

        LAparse(myscanner, &myStatements);

        LAlex_destroy(myscanner);

        std::cout << "Parsing Done" << std::endl;

        for (int i = 0; i < myStatements->size(); i++) {
            std::cout << myStatements->get(i)->toString() << std::endl;
        }
    }
}
