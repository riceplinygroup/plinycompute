// define the service that grabs the sets
(function (angular) {
    'use strict';

    angular.module('app.setsAll', [])
        .factory('setsAll', ['$http', function ($http) {
            return {
                get: function () {
                    return $http({
                        method: 'GET',
                        url: 'api/sets'
                    });
                }
            };
        }]);

}(angular));
