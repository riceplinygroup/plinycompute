// define the service that grabs the types
(function (angular) {
    'use strict';

    angular.module('app.typesAll', [])
        .factory('typesAll', ['$http', function ($http) {
            return {
                get: function () {
                    return $http({
                        method: 'GET',
                        url: 'api/types'
                    });
                }
            };
        }]);

}(angular));

// define the service that grabs a particular type
(function (angular) {
    'use strict';

    angular.module('app.type', [])
        .factory('type', ['$http', function ($http) {
            return {
                get: function (typeID) {
                    return $http({
                        method: 'GET',
                        url: ('api/type/' + typeID)
                    });
                }
            };
        }]);

}(angular));
