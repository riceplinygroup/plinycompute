// define the service that injects the tcap parser
(function (angular) {
    'use strict';

    angular.module('app.tcap-parser', [])
        .factory('tcap-parser', ['$http', function ($http) {
            return tcapParser;
        }]);

}(angular));