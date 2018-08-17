//main.js
angular.module('app')
    .controller('JobDataCtrl', JobDataCtrl)
    .controller('JobTCAPDataCtrl', JobTCAPDataCtrl);

JobDataCtrl.$inject = ['$scope', '$stateParams', 'job'];
function JobDataCtrl($scope, $stateParams, job) {

    $scope.decode = function(text) {
        var entities = [
            ['amp', '&'],
            ['apos', '\''],
            ['#x27', '\''],
            ['#x2F', '/'],
            ['#39', '\''],
            ['#47', '/'],
            ['lt', '<'],
            ['gt', '>'],
            ['nbsp', ' '],
            ['quot', '"']
        ];

        for (var i = 0, max = entities.length; i < max; ++i)
            text = text.replace(new RegExp('&'+entities[i][0]+';', 'g'), entities[i][1]);

        return text;
    };

    job.get($stateParams['jobID']).then(
        function (response) {

            response.data.started = new Date(response.data.started * 1000);
            response.data.ended = new Date(response.data.ended * 1000);
            response.data['tcap-string'] = $scope.decode(response.data['tcap-string']);
            $scope.job = response.data;

            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}

JobTCAPDataCtrl.$inject = ['$scope', '$stateParams', 'VisDataSet', 'job', 'tcap-parser'];
function JobTCAPDataCtrl($scope, $stateParams, VisDataSet, job, parser) {

    $scope.decode = function(text) {
        var entities = [
            ['amp', '&'],
            ['apos', '\''],
            ['#x27', '\''],
            ['#x2F', '/'],
            ['#39', '\''],
            ['#47', '/'],
            ['lt', '<'],
            ['gt', '>'],
            ['nbsp', ' '],
            ['quot', '"']
        ];

        for (var i = 0, max = entities.length; i < max; ++i)
            text = text.replace(new RegExp('&'+entities[i][0]+';', 'g'), entities[i][1]);

        return text;
    };

    job.get($stateParams['jobID']).then(
        function (response) {

            let tcap = $scope.decode(response.data['tcap-string']);


            if (parser.parse(tcap)) {

                let id = 1;

                let openNodes = myPlan.scans;
                let nodeData = [];

                let nodeNameIDs = {};
                let IDtoNode = {};

                // generate the nodes
                while (openNodes.length !== 0) {

                    // get the next node id
                    let curNodeId = id++;

                    // get the current node
                    let node = openNodes[0];

                    if(!(node.output.setName in nodeNameIDs)) {
                        // add a new node
                        nodeData = nodeData.concat([{"id": curNodeId, "label": node.output.setName}]);

                        // add the id for the node
                        IDtoNode[curNodeId] = node;

                        // set the id for the node name
                        nodeNameIDs[node.output.setName] = curNodeId;

                        // grab the consumers
                        let consumers = myPlan.consumers[node.output.setName];

                        // check if we have them
                        if(consumers !== undefined) {

                            // add them to the open nodes
                            openNodes = openNodes.concat(consumers);
                        }
                    }

                    // remove the first element
                    openNodes = openNodes.splice(1);
                }


                openNodes = myPlan.scans;

                let edgeData = [];
                let visitedNodes = {};

                // generate the edges
                while (openNodes.length !== 0) {

                    // get the current node
                    node = openNodes[0];

                    // if we already have visited this node skip it
                    if(node.output.setName in visitedNodes) {

                        // remove the first element
                        openNodes = openNodes.splice(1);

                        // go to the next one
                        continue;
                    }

                    // grab the consumers
                    let consumers = myPlan.consumers[node.output.setName];

                    // check if we have them
                    if(consumers !== undefined) {

                        // go through each consumers
                        for(let i = 0; i < consumers.length; ++i) {
                            edgeData = edgeData.concat([{from: nodeNameIDs[node.output.setName], to: nodeNameIDs[consumers[i].output.setName], "arrows": "to"}]);
                        }

                        // add them to the open nodes
                        openNodes = openNodes.concat(consumers);
                    }

                    // add the nodes to the visited nodes
                    visitedNodes[node.output.setName] = true;

                    // remove the first element
                    openNodes = openNodes.splice(1);
                }

                // create an array with nodes
                let nodes = new vis.DataSet(nodeData);

                // create an array with edges
                let edges = new vis.DataSet(edgeData);

                // provide the data in the vis format
                let data = {
                    nodes: nodes,
                    edges: edges
                };

                let options = {
                    layout: {
                        hierarchical: {
                            sortMethod: "directed",
                            nodeSpacing : 400
                        }
                    },
                    physics: {
                        enabled: false
                    },
                    edges: {
                        smooth: true,
                        arrows: {to : true }
                    },
                    height: "800px"
                };

                $scope.data = data;
                $scope.options = options;
            }

            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}