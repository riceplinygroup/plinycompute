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
        class TranslateNode : public QueryAlgo
        {
        public:

            TranslateNode(Handle<QueryBase> queryNodeParam, list<string>& tcapLines, int &nextUnusedTableName,
                          Handle<Map<Handle<QueryBase>, int>> nodeToResultTableName)
                    : _node(queryNodeParam), _tcapLines(tcapLines), _nextUnusedTableName(nextUnusedTableName),
                      _nodeToResultTableName(nodeToResultTableName)
            {
            }

            void forSelection(QueryBase&) override  // node is a Selection<?,?>
            {

                Handle <QueryBase> parentNode = _node->getIthInput(0);
                buildTcap(parentNode, _tcapLines, _nextUnusedTableName, _nodeToResultTableName);
                string parentResultTableName = std::to_string(_nodeToResultTableName->operator[](parentNode));

                Handle<Selection<Nothing,Nothing>> selection = unsafeCast<Selection<Nothing,Nothing>>(_node);
                Handle<Nothing> placeholder = makeObject<Nothing>();
                SimpleLambda<bool> filterPred = selection->getSelection(placeholder);

                _tcapLines.push_back("@exec filterIdent");

                _nodeToResultTableName->operator[](_node) = _nextUnusedTableName;
                string resultTableName = std::to_string(_nextUnusedTableName);
                _nextUnusedTableName++;


                _tcapLines.push_back(resultTableName + "(1) = apply func \"f\" to " + parentResultTableName + " retain none");

            }

            void forSet(QueryBase&) override // node is a Set<?> which is a user query graph root
            {

                string dbName = _node->getDBName();
                string setName = _node->getSetName();

                _nodeToResultTableName->operator[](_node) = _nextUnusedTableName;
                string resultTableName = std::to_string(_nextUnusedTableName);
                _nextUnusedTableName++;
                _tcapLines.push_back(resultTableName + "(1) = load \"" + dbName + " " + setName + "\"");
//
//                output = make_shared<SourceSetNameIr>(dbName, setName); // SourceSetNameIr is our root type
            }

            void forQueryOutput(QueryBase&) override // node is a QueryOutput<?> which represents a sink
            {
                Handle <QueryBase> parentNode = _node->getIthInput(0); // QueryOutput has only one input contractually.
                buildTcap(parentNode, _tcapLines, _nextUnusedTableName, _nodeToResultTableName);

                string dbName = _node->getDBName();
                string setName = _node->getSetName();

                string parentResultTableName = std::to_string(_nodeToResultTableName->operator[](parentNode));
                _tcapLines.push_back("store " + parentResultTableName + " \"" + dbName + " " + setName + "\"");

            }


        private:


            Handle<QueryBase> _node; // function's node param

            Handle<Map<Handle<QueryBase>, int>> _nodeToResultTableName;

            list<string>& _tcapLines;

            int& _nextUnusedTableName;

        } trans(node, tcapLines, nextUnusedTableName, nodeToResultTableName);

        node->execute(trans);

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