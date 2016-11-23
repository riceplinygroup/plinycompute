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
#include "TcapBuilder.h"
#include <list>

#include "Handle.h"
#include "QueryAlgo.h"
#include "QueryBase.h"
#include "Object.h"
#include "PDBMap.h"
#include "Selection.h"

using std::list;

using pdb::Handle;
using pdb::Object;
using pdb::QueryAlgo;
using pdb::QueryBase;
using pdb::makeObject;
using pdb::Nothing;
using pdb::Selection;
using pdb::SimpleLambda;
using pdb::Map;
using pdb::unsafeCast;

namespace pdb_detail
{
    void buildTcap(QueryBaseHdl node, list<string> &tcapLines, int &nextUnusedTableName,
                   Handle<Map<Handle<QueryBase>, int>> nodeToResultTableName)
    {

        node->match([&](QueryBase&) // Selection case
                    {
                        Handle <QueryBase> parentNode = node->getIthInput(0);
                        buildTcap(parentNode, tcapLines, nextUnusedTableName, nodeToResultTableName);
                        string parentResultTableName = std::to_string(nodeToResultTableName->operator[](parentNode));

                        Handle<Selection<Nothing,Nothing>> selection = unsafeCast<Selection<Nothing,Nothing>>(node);
                        Handle<Nothing> placeholder = makeObject<Nothing>();
                        SimpleLambda<bool> filterPred = selection->getSelection(placeholder);

                        tcapLines.push_back("@exec filterIdent");

                        nodeToResultTableName->operator[](node) = nextUnusedTableName;
                        string resultTableName = std::to_string(nextUnusedTableName);
                        nextUnusedTableName++;

                        tcapLines.push_back(resultTableName + "(1) = apply func \"f\" to " + parentResultTableName + " retain none");
                    },
                    [&](QueryBase&) // Set case
                    {
                        string dbName = node->getDBName();
                        string setName = node->getSetName();

                        nodeToResultTableName->operator[](node) = nextUnusedTableName;
                        string resultTableName = std::to_string(nextUnusedTableName);
                        nextUnusedTableName++;
                        tcapLines.push_back(resultTableName + "(1) = load \"" + dbName + " " + setName + "\"");
    //
    //                output = make_shared<SourceSetNameIr>(dbName, setName); // SourceSetNameIr is our root type
                    },
                    [&](QueryBase&) // QueryOutput case
                    {
                        Handle <QueryBase> parentNode = node->getIthInput(0); // QueryOutput has only one input contractually.
                        buildTcap(parentNode, tcapLines, nextUnusedTableName, nodeToResultTableName);

                        string dbName = node->getDBName();
                        string setName = node->getSetName();

                        string parentResultTableName = std::to_string(nodeToResultTableName->operator[](parentNode));
                        tcapLines.push_back("store " + parentResultTableName + " \"" + dbName + " " + setName + "\"");
                    });
    }

    string buildTcap(QueryBaseHdl node)
    {
        if(node.isNullPtr())
            return "";

        list<string> tcapLines;

        int tableIdentStart = 1;
        Handle<Map<Handle<QueryBase>, int>> nodeToResultTableName = makeObject<Map<Handle<QueryBase>, int>>();
        buildTcap(node, tcapLines, tableIdentStart, nodeToResultTableName);

        string program = "";
        for(string tcapLine : tcapLines)
            program+=tcapLine + "\n";

        return program;


    }
}