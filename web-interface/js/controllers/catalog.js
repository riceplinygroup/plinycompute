//main.js
angular.module('app')
    .controller('SetsDataCtrl', SetsDataCtrl);


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