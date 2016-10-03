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

#ifndef SELECTION_CC
#define SELECTION_CC

#include "Selection.h"
#include "SelectionQueryProcessor.h"
#include "FilterQueryProcessor.h"
#include "ProjectionQueryProcessor.h"
#include "SimpleSingleTableQueryProcessor.h"
#include <memory>

namespace pdb {

template <typename Out, typename In> 
SimpleSingleTableQueryProcessorPtr Selection <Out, In> :: getProcessor () {
	return std :: make_shared <SelectionQueryProcessor <Out, In>> (*this);
}

template <typename Out, typename In>
SimpleSingleTableQueryProcessorPtr Selection <Out, In> :: getFilterProcessor () {
        return std :: make_shared <FilterQueryProcessor <Out, In>> (*this);
}


template <typename Out, typename In>
SimpleSingleTableQueryProcessorPtr Selection <Out, In> :: getProjectionProcessor () {
        return std :: make_shared <ProjectionQueryProcessor <Out, In>> (*this);
}




	template <typename In, typename Out>
	void Selection<In, Out>::execute(QueryAlgo& algo)
	{
		algo.forSelection(*this);
	};

}

#endif
