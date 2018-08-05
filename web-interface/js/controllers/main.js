//main.js
angular.module('app')
       .controller('DashboardDataCtrl', DashboardDataCtrl)
       .controller('MemoryPieCtrl', MemoryPieCtrl)
       .controller('CPUPieCtrl', CPUPieCtrl);

CPUPieCtrl.$inject = ['$scope', 'clusterInfo'];
function CPUPieCtrl($scope, clusterInfo) {
    clusterInfo.get().then(
        function (response) {

            // the data we are going to put in the scope
            var labels = [];
            var data = [];

            // grab the nodes
            var nodes = response.data.nodes;

            // go through each node
            for (var i = 0; i < nodes.length; i++) {
                labels.push("Node " + nodes[i]["node-id"]);
                data.push(nodes[i]["number-of-cores"]);
            }

            // set the data to the scope
            $scope.labels = labels;
            $scope.data = data;

            // log the response
            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}

MemoryPieCtrl.$inject = ['$scope', 'clusterInfo'];
function MemoryPieCtrl($scope, clusterInfo) {

    clusterInfo.get().then(
        function (response) {

            // the data we are going to put in the scope
            var labels = [];
            var data = [];

            // grab the nodes
            var nodes = response.data.nodes;

            // go through each node
            for (var i = 0; i < nodes.length; i++) {
                labels.push("Node " + nodes[i]["node-id"]);
                data.push(nodes[i]["memory-size"] / 1024);
            }

            // set the data to the scope
            $scope.labels = labels;
            $scope.data = data;

            // log the response
            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}

DashboardDataCtrl.$inject = ['$scope', 'clusterInfo'];
function DashboardDataCtrl($scope, clusterInfo) {

    clusterInfo.get().then(
        function (response) {
            $scope.clusterInfo = response.data;
            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}
