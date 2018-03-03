
#ifndef QUERY_H
#define QUERY_H

#include "Handle.h"
#include "Object.h"
#include "QueryBase.h"
#include "PDBVector.h"
#include "TypeName.h"

namespace pdb {

// this is the basic query type... all queries returning OutType derive from this class
template <typename OutType>
class Query : public QueryBase {

public:
    Query() {
        myOutType = getTypeName<OutType>();
    }

    // gets the name of this output type
    std::string getOutputType() override {
        return myOutType;
    }

    // from QueryBase
    // virtual int getNumInputs () = 0;
    // virtual std :: string getIthInputType (int i) = 0;
    // virtual std :: string getQueryType () = 0;

    String myOutType;
};
}

#endif
