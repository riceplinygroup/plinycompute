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
#ifndef LA_ASTNODE_H
#define LA_ASTNODE_H

#define LA_ASTNODE_TYPE_IDENTIFIER 1
#define LA_ASTNODE_TYPE_INITIALIZER 2
#define LA_ASTNODE_TYPE_PRIMARYEXPRESSION 3
#define LA_ASTNODE_TYPE_POSTFIXEXPRESSION 4
#define LA_ASTNODE_TYPE_MULTIPLICATIVEEXPRESSION 5
#define LA_ASTNODE_TYPE_ADDITIVEEXPRESSION 6
#define LA_ASTNODE_TYPE_STATEMENT 7

#include <iostream>
#include <memory>

// by Binhang, June 2017

struct LAASTNode;
typedef std::shared_ptr<struct LAASTNode> LAASTNodePtr;


struct LAASTNode {
private:
    int type;

protected:
    LAASTNode(int t) : type(t) {}

public:
    virtual std::string toString() = 0;

    int getType() {
        return type;
    }
};

#endif