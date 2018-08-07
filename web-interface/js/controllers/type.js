//main.js
angular.module('app').controller('TypeDataCtrl', TypeDataCtrl);

TypeDataCtrl.$inject = ['$scope', '$stateParams', 'type'];
function TypeDataCtrl($scope, $stateParams, type) {

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

    type.get($stateParams['typeID']).then(
        function (response) {

            $scope.type = response.data;
            console.log(response);
        },

        function (error) {
            console.log(error);
        }
    );
}