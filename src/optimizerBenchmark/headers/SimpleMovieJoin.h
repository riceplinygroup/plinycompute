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
#ifndef SIMPLE_MOVIE_JOIN_H
#define SIMPLE_MOVIE_JOIN_H

//by Sourav, Jun 2017

#include "JoinComp.h"
#include "LambdaCreationFunctions.h"


#include "MovieStar.h"
#include "StarsIn.h"



using namespace pdb;

class SimpleMovieJoin : public JoinComp <MovieStar, MovieStar, StarsIn> {

public:

        ENABLE_DEEP_COPY

        SimpleMovieJoin () {}

        Lambda <bool> getSelection (Handle <MovieStar> in1, Handle <StarsIn> in2) override {
                return (makeLambdaFromMember (in1, name) == makeLambdaFromMember (in2, starName));
        }

        Lambda <Handle <MovieStar>> getProjection (Handle <MovieStar> in1, Handle <StarsIn> in2) override {
            return makeLambda (in1, [](Handle<MovieStar> & in1) {
                  return in1;
            });
        }
};

#endif
