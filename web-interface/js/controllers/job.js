//main.js
angular.module('app')
    .controller('JobDataCtrl', JobDataCtrl);

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