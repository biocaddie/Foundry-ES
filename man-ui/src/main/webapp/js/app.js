var app = angular.module('man-ui', ['ui.router', 'ui.bootstrap', 'ui.ace', 'smart-table',
    'foundryServices', 'foundryControllers', 'foundryDirectives']);

app.config(['$stateProvider', '$urlRouterProvider',
    function ($stateProvider, $urlRouterProvider, $injector) {
        $urlRouterProvider.otherwise(function ($injector) {
            var $state = $injector.get("$state");
            $state.go("welcome");
        });

        $stateProvider.state('sources',
            {
                url: "/sources",
                templateUrl: 'partials/source-panel.html',
                controller: 'SourcesController',
                resolve: {
                    sourceSummaryList: function (dataService) {
                        return dataService.getSourceSummaries();
                    },
                    icConfigs: function (dataService) {
                        return dataService.harvest();
                    }
                },
                data: {
                    requireLogin: true
                }
            }
        ).state("transforms", {
            url: '/transforms',
            templateUrl: 'partials/transformation-panel.html',
            controller: 'TransformationController',
            resolve: {
                sourceSummaryList: function (dataService) {
                    return dataService.getSourceSummaries();
                }
            },
            data: {
                requireLogin: true
            }
        }).state("welcome", {
            url: "/",
            templateUrl: 'partials/welcome.html',
            data: {
                requireLogin: false
            }
        }).state("dashboard", {
            url: "/dashboard",
            templateUrl: 'partials/dashboard-panel.html',
            controller: 'DashboardController',
            resolve: {
                sourceProcStatsList: function (dataService) {
                    return dataService.getStats();
                }
            },
            data: {requireLogin: true}
        }).state("users", {
            url: "/users",
            templateUrl: "partials/user-list.html",
            controller: 'UsersController',
            resolve: {
                userList: function (dataService) {
                    return dataService.getUsers();
                }
            },
            data: {requireLogin: true}
        }).state('userDetail', {
            url: "/user/:username",
            templateUrl: 'partials/user-detail.html',
            controller: 'UserDetailController',
            data: {requireLogin: true}
        }).state('transform', {
            url: "/transform",
            templateUrl: 'partials/transform-panel.html',
            controller: 'TransformController',
            data: {requireLogin: false}
        });
    }]);


app.run(function ($rootScope, $state, $stateParams, loginService) {
    $rootScope.$state = $state;
    $rootScope.$stateParams = $stateParams;
    /* for TEST */
    $rootScope.$on('$stateChangeStart', function (event, toState, toParams) {
        var requireLogin = toState.data.requireLogin;
        if (requireLogin && !$rootScope.currentUser) {
            event.preventDefault();
            loginService.open()
                .then(function () {
                    return $state.go(toState.name, toParams);
                })
                .catch(function (reason) {
                    console.log('reason:' + reason);
                    $rootScope.currentUser = null;
                    return $state.go('sources');
                });
        }
    });
});

app.filter('startFrom', function () {
    return function (input, startIdx) {
        if (input) {
            startIdx = +startIdx;
            return input.slice(startIdx);
        }
        return [];
    }
});

