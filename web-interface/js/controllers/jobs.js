//main.js
angular.module('app')
    .controller('JobsDataCtrl', JobsDataCtrl);

JobsDataCtrl.$inject = ['$scope', 'jobsAll'];
function JobsDataCtrl($scope, jobsAll) {

    jobsAll.get().then(
        function (response) {

            // fix the timestamp
            for(var i = 0; i < response.data.length; ++i) {
                response.data[i].started = new Date(response.data[i].started * 1000);
                response.data[i].ended = new Date(response.data[i].ended * 1000);
            }

            $scope.jobs = response.data;
            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}