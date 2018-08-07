//main.js
angular.module('app')
       .controller('SetDataCtrl', SetDataCtrl)
       .controller('SetPieCtrl', SetPieCtrl);

SetDataCtrl.$inject = ['$scope', '$stateParams', 'set'];
function SetDataCtrl($scope, $stateParams, set) {

    set.get($stateParams['dbName'], $stateParams['setName']).then(
        function (response) {

            $scope.set = response.data;
            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}

SetPieCtrl.$inject = ['$scope', '$stateParams', 'set'];
function SetPieCtrl($scope, $stateParams, set) {

    set.get($stateParams['dbName'], $stateParams['setName']).then(
        function (response) {

            $scope.set = response.data;

            // the data we are going to put in the scope
            var labels = [];
            var data = [];

            // grab the partitions
            var partitions = response.data["partitions"];

            // go through each node
            for (var i = 0; i < partitions.length; i++) {
                labels.push("Node " + partitions[i]["node-id"]);
                data.push(partitions[i]["size"]);
            }

            // set the data to the scope
            $scope.labels = labels;
            $scope.data = data;

            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}

