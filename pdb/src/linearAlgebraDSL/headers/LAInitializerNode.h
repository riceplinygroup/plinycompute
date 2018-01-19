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
#ifndef LA_INITIALIZER_NODE_H
#define LA_INITIALIZER_NODE_H

#include "LAASTNode.h"
#include "LAExpressionNode.h"

// by Binhang, June 2017

struct LAInitializerNode;
typedef std::shared_ptr<struct LAInitializerNode> LAInitializerNodePtr;


struct LAInitializerNode : public LAExpressionNode {

private:
    std::string method;
    pdb::Handle<pdb::Computation> scanSet;
    LAInitializerNodePtr me;
    std::string path;
    LADimension dim;

public:
    LAInitializerNode(const char* methodTag, int rs, int cs, int rn, int cn)
        : LAExpressionNode(LA_ASTNODE_TYPE_INITIALIZER), dim(rs, cs, rn, cn) {
        method = methodTag;
    }


    pdb::Handle<pdb::Computation>& evaluate(LAPDBInstance& instance) final;


    std::string toString() {
        if (method.compare("zeros") == 0) {
            return "zeros(" + std::to_string(dim.blockRowSize) + "," +
                std::to_string(dim.blockColSize) + "," + std::to_string(dim.blockRowNum) + "," +
                std::to_string(dim.blockColNum) + ")";
        } else if (method.compare("ones") == 0) {
            return "ones(" + std::to_string(dim.blockRowSize) + "," +
                std::to_string(dim.blockColSize) + "," + std::to_string(dim.blockRowNum) + "," +
                std::to_string(dim.blockColNum) + ")";
        } else if (method.compare("identity") == 0) {
            return "identity(" + std::to_string(dim.blockRowSize) + "," +
                std::to_string(dim.blockColSize) + "," + std::to_string(dim.blockRowNum) + "," +
                std::to_string(dim.blockColNum) + ")";
        } else if (method.compare("load") == 0) {
            return "load(" + std::to_string(dim.blockRowSize) + "," +
                std::to_string(dim.blockColSize) + "," + std::to_string(dim.blockRowNum) + "," +
                std::to_string(dim.blockColNum) + "," + path + ")";
        } else {
            return "Initializer invalid method: " + method;
        }
    }

    void setShared(LAInitializerNodePtr meIn) {
        me = meIn;
    }

    void setLoadPath(char* p) {
        path = p;
    }

    bool isSyntaxSugarInitializer() {
        return true;
    }

    LADimension getDimension() {
        return dim;
    }
};

#endif