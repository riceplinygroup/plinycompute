
// define the service that grabs the cluster info
(function (angular) {
    'use strict';

    angular.module('app.jobsAll', [])
        .factory('jobsAll', ['$http', function ($http) {
            return {
                get: function () {
                    return $http({
                        method: 'GET',
                        url: 'api/jobs'
                    });
                }
            };
        }]);

}(angular));

// define the service that grabs a particular set
(function (angular) {
    'use strict';

    angular.module('app.job', [])
        .factory('job', ['$http', function ($http) {
            return {
                get: function (jobID) {
                    return $http({
                        method: 'GET',
                        url: ('api/job/' + jobID)
                    });
                }
            };
        }]);

}(angular));