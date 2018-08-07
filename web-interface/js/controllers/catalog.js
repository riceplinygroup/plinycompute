//main.js
angular.module('app')
    .controller('SetsDataCtrl', SetsDataCtrl)
    .controller('TypesDataCtrl', TypesDataCtrl);


SetsDataCtrl.$inject = ['$scope', 'setsAll'];
function SetsDataCtrl($scope, setsAll) {

    setsAll.get().then(
        function (response) {

            // fix the timestamp
            for(var i = 0; i < response.data.length; ++i) {
                response.data[i].created = new Date(response.data[i].created * 1000);
            }

            $scope.sets = response.data;
            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}

TypesDataCtrl.$inject = ['$scope', 'typesAll'];
function TypesDataCtrl($scope, typesAll) {

    typesAll.get().then(
        function (response) {

            // fix the timestamp
            for(var i = 0; i < response.data.length; ++i) {
               response.data[i].registered = new Date(response.data[i].registered * 1000);
            }

            $scope.types = response.data;
            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}