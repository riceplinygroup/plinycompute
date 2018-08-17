angular
.module('app')
.config(['$stateProvider', '$urlRouterProvider', '$ocLazyLoadProvider', '$breadcrumbProvider', function($stateProvider, $urlRouterProvider, $ocLazyLoadProvider, $breadcrumbProvider) {

  $urlRouterProvider.otherwise('/dashboard');

  $ocLazyLoadProvider.config({
    // Set to true if you want to see what and when is dynamically loaded
    debug: true
  });

  $breadcrumbProvider.setOptions({
    prefixStateName: 'app.main',
    includeAbstract: true,
    template: '<li class="breadcrumb-item" ng-repeat="step in steps" ng-class="{active: $last}" ng-switch="$last || !!step.abstract"><a ng-switch-when="false" href="{{step.ncyBreadcrumbLink}}">{{step.ncyBreadcrumbLabel}}</a><span ng-switch-when="true">{{step.ncyBreadcrumbLabel}}</span></li>'
  });

  $stateProvider
  .state('app', {
    abstract: true,
    templateUrl: 'views/common/layouts/full.html',
    //page title goes here
    ncyBreadcrumb: {
      label: 'Root',
      skip: true
    },
    resolve: {
      loadCSS: ['$ocLazyLoad', function($ocLazyLoad) {
        // you can lazy load CSS files
        return $ocLazyLoad.load([{
          serie: true,
          name: 'Flags',
          files: ['node_modules/flag-icon-css/css/flag-icon.min.css']
        },{
          serie: true,
          name: 'Font Awesome',
          files: ['node_modules/font-awesome/css/font-awesome.min.css']
        },{
          serie: true,
          name: 'Simple Line Icons',
          files: ['node_modules/simple-line-icons/css/simple-line-icons.css']
        }]);
      }],
      loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {
        // you can lazy load files for an existing module
        return $ocLazyLoad.load([{
          serie: true,
          name: 'chart.js',
          files: [
            'node_modules/chart.js/dist/Chart.min.js',
            'node_modules/angular-chart.js/dist/angular-chart.min.js'
          ]
        }]);
      }]
    }
  })
  .state('app.main', {
    url: '/dashboard',
    templateUrl: 'views/main.html',
    //page title goes here
    ncyBreadcrumb: {
      label: 'PlinyCompute Web Interface'
    },
    //page subtitle goes here
    params: { subtitle: 'Welcome to the PlinyCompute Dashboard' },
    resolve: {
      loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {

        // you can lazy load files for an existing module
        return $ocLazyLoad.load([
          {
            serie: true,
            name: 'chart.js',
            files: [
              'node_modules/chart.js/dist/Chart.min.js',
              'node_modules/angular-chart.js/dist/angular-chart.min.js'
            ]
          }
        ]);
      }],
      loadMyCtrl: ['$ocLazyLoad', function($ocLazyLoad) {
        // you can lazy load controllers
        return $ocLazyLoad.load({
          files: ['js/controllers/main.js', 'js/services/cluster-info.js']
        });
      }]
    }
  })
  .state('app.catalog', {
      url: '/catalog',
      templateUrl: 'views/catalog.html',
      //page title goes here
      ncyBreadcrumb: {
          label: 'PlinyCompute Catalog'
      },
      //page subtitle goes here
      params: { subtitle: 'Explore the Catalog' },
      resolve: {
          loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {

              // you can lazy load files for an existing module
              return $ocLazyLoad.load([
                  {
                      serie: true,
                      name: 'chart.js',
                      files: [
                          'node_modules/chart.js/dist/Chart.min.js',
                          'node_modules/angular-chart.js/dist/angular-chart.min.js'
                      ]
                  }
              ]);
          }],
          loadMyCtrl: ['$ocLazyLoad', function($ocLazyLoad) {
              // you can lazy load controllers
              return $ocLazyLoad.load({
                  files: ['js/controllers/catalog.js', 'js/services/sets.js', 'js/services/types.js']
              });
          }]
      }
  })
  .state('app.set', {
      url: '/set?dbName&setName',
      templateUrl: 'views/catalog-set.html',
      //page title goes here
      ncyBreadcrumb: {
          label: 'PlinyCompute Set'
      },
      //page subtitle goes here
      params: { subtitle: 'Explore the Set' },
      resolve: {
          loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {

              // you can lazy load files for an existing module
              return $ocLazyLoad.load([
                  {
                      serie: true,
                      name: 'chart.js',
                      files: [
                          'node_modules/chart.js/dist/Chart.min.js',
                          'node_modules/angular-chart.js/dist/angular-chart.min.js'
                      ]
                  }
              ]);
          }],
          loadMyCtrl: ['$ocLazyLoad', function($ocLazyLoad) {
              // you can lazy load controllers
              return $ocLazyLoad.load({
                  files: ['js/controllers/set.js', 'js/services/sets.js']
              });
          }]
      }
  })
  .state('app.type', {
      url: '/type?typeID',
      templateUrl: 'views/catalog-type.html',
      //page title goes here
      ncyBreadcrumb: {
          label: 'PlinyCompute Type'
      },
      //page subtitle goes here
      params: { subtitle: 'Explore the Type' },
      resolve: {
          loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {

              // you can lazy load files for an existing module
              return $ocLazyLoad.load([
                  {
                      serie: true,
                      name: 'chart.js',
                      files: [
                          'node_modules/chart.js/dist/Chart.min.js',
                          'node_modules/angular-chart.js/dist/angular-chart.min.js'
                      ]
                  }
              ]);
          }],
          loadMyCtrl: ['$ocLazyLoad', function($ocLazyLoad) {
              // you can lazy load controllers
              return $ocLazyLoad.load({
                  files: ['js/controllers/type.js', 'js/services/types.js']
              });
          }]
      }
  })
  .state('app.jobs', {
      url: '/jobs',
      templateUrl: 'views/jobs.html',
      //page title goes here
      ncyBreadcrumb: {
          label: 'PlinyCompute Jobs'
      },
      //page subtitle goes here
      params: { subtitle: 'Explore the Jobs' },
      resolve: {
          loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {

              // you can lazy load files for an existing module
              return $ocLazyLoad.load([
                  {
                      serie: true,
                      name: 'chart.js',
                      files: [
                          'node_modules/chart.js/dist/Chart.min.js',
                          'node_modules/angular-chart.js/dist/angular-chart.min.js'
                      ]
                  }
              ]);
          }],
          loadMyCtrl: ['$ocLazyLoad', function($ocLazyLoad) {
              // you can lazy load controllers
              return $ocLazyLoad.load({
                  files: ['js/controllers/jobs.js', 'js/services/jobs.js']
              });
          }]
      }
  })
  .state('app.job', {
      url: '/job?jobID',
      templateUrl: 'views/jobs-job.html',
      //page title goes here
      ncyBreadcrumb: {
          label: 'PlinyCompute Job'
      },
      //page subtitle goes here
      params: { subtitle: 'Explore the Job' },
      resolve: {
          loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {

              // you can lazy load files for an existing module
              return $ocLazyLoad.load([
                  {
                      serie: true,
                      name: 'chart.js',
                      files: [
                          'node_modules/chart.js/dist/Chart.min.js',
                          'node_modules/angular-chart.js/dist/angular-chart.min.js'
                      ]
                  }
              ]);
          }],
          loadMyCtrl: ['$ocLazyLoad', function($ocLazyLoad) {
              // you can lazy load controllers
              return $ocLazyLoad.load({
                  files: ['js/controllers/job.js', 'js/services/jobs.js', 'js/services/tcap-parser/tcap-parser.js', 'js/services/parser.js']
              });
          }]
      }
  })
}]);
