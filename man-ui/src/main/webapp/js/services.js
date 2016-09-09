var foundryServices = angular.module('foundryServices', []);

app.factory("transformRequestAsFormPost", function () {
    function transformRequest(data, getHeaders) {
        return ( serializeData(data) );
    }

    function serializeData(data) {
        if (!angular.isObject(data)) {
            return ( ( data == null ) ? "" : data.toString() );
        }
        var buffer = [];
        // Serialize each key in the object.
        for (var name in data) {
            if (!data.hasOwnProperty(name)) {
                continue;
            }
            var value = data[name];
            buffer.push(encodeURIComponent(name) + "=" +
                encodeURIComponent(( value == null ) ? "" : value)
            );
        }
        // Serialize the buffer and clean it up for transportation.
        var source = buffer.join("&").replace(/%20/g, "+");
        // console.log('source:' + source);
        return source;
    }

    return transformRequest;
});

foundryServices.service('dataService', ['$rootScope', '$http', 'transformRequestAsFormPost',
    function ($rootScope, $http, transformRequestAsFormPost) {
        this.login = function (loginId, pwd) {
            // console.log('login:: loginId:' + loginId + "pwd:" + pwd);
            return $http({
                method: 'post', url: '/foundry/api/user/login',
                transformRequest: transformRequestAsFormPost,
                data: {
                    user: loginId, pwd: pwd
                }, headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });
        };
        this.getStats = function () {
            return $http.get('/foundry/api/dashboard/stats', {
                params: {
                    apiKey: $rootScope.currentUser.apiKey,
                    refresh: true
                },
                headers: {'Accept': 'application/json'}
            });
        };
        this.getUsers = function () {
            return $http.get('/foundry/api/user/users', {
                params: {apiKey: $rootScope.currentUser.apiKey},
                headers: {'Accept': 'application/json'}
            });
        };

        this.findUser = function (username) {
            return $http.get('/foundry/api/user/users/' + username, {
                params: {apiKey: $rootScope.currentUser.apiKey},
                headers: {'Accept': 'application/json'}
            });
        };

        this.saveUser = function (payload) {
            return $http({
                method: 'post', url: '/foundry/api/user/users',
                transformRequest: transformRequestAsFormPost,
                data: {
                    apiKey: $rootScope.currentUser.apiKey,
                    payload: JSON.stringify(payload)
                },
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });
        };
        this.getSourceSummaries = function () {
            return $http.get('/foundry/api/sources', {
                params: {apiKey: $rootScope.currentUser.apiKey},
                headers: {'Accept': 'application/json'}
            });
        };
        this.getSourceInfo = function (selSource) {
            return $http.get('/foundry/api/sources/source', {
                params: {
                    apiKey: $rootScope.currentUser.apiKey,
                    sourceID: selSource.sourceID,
                    dataSource: selSource.dataSource
                },
                headers: {'Accept': 'application/json'}
            });
        };
        this.harvest = function () {
            return $http.get('/foundry/api/harvest', {
                params: {apiKey: $rootScope.currentUser.apiKey},
                headers: {'Accept': 'application/json'}
            });
        };
        this.saveSourceDesc = function (payload) {
            return $http({
                method: 'post', url: '/foundry/api/sources',
                transformRequest: transformRequestAsFormPost,
                data: {
                    apiKey: $rootScope.currentUser.apiKey,
                    payload: JSON.stringify(payload)
                },
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });
        };

        this.updateSourceDesc = function (payload) {
            return $http({
                method: 'post', url: '/foundry/api/sources/update',
                transformRequest: transformRequestAsFormPost,
                data: {
                    apiKey: $rootScope.currentUser.apiKey,
                    payload: JSON.stringify(payload)
                },
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });
        };

        this.testTransformation = function (sourceId, dataSource, transformScript) {
            return $http({
                method: 'post', url: '/foundry/api/sources/testTransform',
                transformRequest: transformRequestAsFormPost,
                data: {
                    apiKey: $rootScope.currentUser.apiKey,
                    sourceId: sourceId,
                    dataSource: dataSource, transformScript: transformScript
                },
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });
        };

        this.transform = function (apiKey, transformScript, sampleData) {
            return $http({
                method: 'post', url: '/foundry/api/transformation/transform',
                transformRequest: transformRequestAsFormPost,
                data: {
                    apiKey: apiKey,
                    jsonInput: sampleData,
                    transformationScript: transformScript
                },
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });
        };
        this.testRule = function (transformRule, sampleData) {
            return $http({
                method: 'post', url: '/foundry/api/transformation/testrule',
                transformRequest: transformRequestAsFormPost,
                data: {
                    apiKey: $rootScope.currentUser.apiKey,
                    jsonInput: sampleData,
                    transformationRule: transformRule
                },
                headers: {
                    'Accept': 'application/json',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });
        };
        this.sample = function (selSource) {
            return $http.get('/foundry/api/sources/sample', {
                params: {
                    apiKey: $rootScope.currentUser.apiKey,
                    sourceId: selSource.sourceID,
                    dataSource: selSource.dataSource
                },
                headers: {'Accept': 'application/json'}
            });
        }
    }]);

foundryServices.service('helpService', function ($uibModal) {
    this.openApplyHelp = function () {
        var instance = $uibModal.open({
            templateUrl: 'partials/script_apply_help.html',
            controller: 'HelpController'
        });
        return instance.result.then(function () {
            console.log('Modal dismissed');
        });
    };
    this.openJSONTLHelp = function () {
        var instance = $uibModal.open({
            templateUrl: 'partials/jsontl_syntax_help.html',
            controller: 'HelpController',
            windowClass:'jsontl-modal-window'
        });
        return instance.result.then(function () {
            console.log('Modal dismissed');
        });
    };

});
foundryServices.service('loginService', function ($rootScope, $uibModal) {
    this.open = function () {
        var instance = $uibModal.open({
            templateUrl: 'partials/login.html',
            controller: 'LoginController',
            controllerAs: 'loginCtrl'
        });
        return instance.result.then(function (user) {
            console.log("loginService:: user:" + user);
            $rootScope.currentUser = user;
            return user;
        }, function () {
            console.log('Modal dismissed');
        });
    };

});
