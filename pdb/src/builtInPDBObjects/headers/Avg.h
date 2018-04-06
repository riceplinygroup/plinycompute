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


#ifndef AVG_H
#define AVG_H


#include "Object.h"

// PRELOAD %Avg%

namespace pdb {

class Avg : public Object {

public:
    double total;
    int count;

    ENABLE_DEEP_COPY


    Avg () {}

    Avg (double total, int count) {
        this->total = total;
        this->count = count;
    }

    double getAvg() {
        return total/(double)(count);
    }

    Avg & operator+ (Avg& avg) {
        this->total += avg.total;
        this->count += avg.count;
        return *this;
    }
};
}


#endif
