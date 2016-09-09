/**
 * Created by bozyurt on 11/8/15.
 */

var foundryCtrls = angular.module('foundryControllers', ['foundryServices', 'ui.bootstrap']);

foundryCtrls.controller('LoginController', ['$scope', '$uibModalInstance', 'dataService',
    function ($scope, $uibModalInstance, dataService) {
        $scope.cancel = function () {
            $uibModalInstance.dismiss('cancel');
        };

        $scope.ok = function (loginId, password) {
            // console.log("ok:: loginId:" + loginId + " pwd:" + password);
            dataService.login(loginId, password).then(function (response) {
                var user = {};
                user.apiKey = response.data.apiKey;
                user.name = loginId;
                $uibModalInstance.close(user);
            }, function (response) {
                console.dir(response);
                $uibModalInstance.close(null);
            });
        }
    }]);

foundryCtrls.controller('MenuBarController', ['$scope', '$rootScope', '$state',
    function ($scope, $rootScope, $state) {
        $scope.logoff = function () {
            $rootScope.currentUser = null;
            $state.go("welcome");
        };
    }]);

foundryCtrls.controller('UsersController', ['$scope', '$state', 'userList',
    function ($scope, $state, userList) {
        $scope.userList = userList.data;
        $scope.showUser = function (user) {
            $state.go("userDetail", {username: user.username});
        };
        $scope.showAddUser = function () {
            $state.go("userDetail", {username: ''});
        }
    }]);

foundryCtrls.controller("UserDetailController", ['$scope', '$state', '$stateParams', 'dataService',
    function ($scope, $state, $stateParams, dataService) {
        var username = $stateParams.username;
        $scope.user = {};
        $scope.updateMode = !!username;
        if ($scope.updateMode) {
            dataService.findUser(username)
                .then(function (response) {
                    $scope.user = response.data;
                }, function (reason) {
                    console.dir(reason);
                    alert("An error occurred while retrieving the user '" + username + "'!");
                });
        }

        $scope.saveUser = function () {
            var payload = {opMode: 'new', user: $scope.user};
            if ($scope.updateMode) {
                payload.opMode = 'update';
            }
            dataService.saveUser(payload)
                .then(function (response) {
                    alert("User information is successfully saved!");
                    $state.go("users");
                }, function (reason) {
                    console.dir(reason);
                    alert("An error occurred while saving user information:" + reason);
                });
        };
        $scope.cancel = function () {
            $state.go("users");
        };
    }]);

foundryCtrls.controller('SourcesController', ['$scope', 'dataService', 'sourceSummaryList', 'icConfigs',
    function ($scope, dataService, sourceSummaryList, icConfigs) {
        $scope.ssList = sourceSummaryList.data;
        $scope.icConfigs = icConfigs.data;
        console.log('sourceSummaryList:' + $scope.ssList);
        $scope.state = {selSourceId: -1, selHarvestId: -1};
        $scope.ss = {};
        $scope.opMode = 'new';

        $scope.sourceChanged = function () {
            console.log('sourceChanged:' + $scope.state.selSourceId);
            var ss = $scope.ssList[$scope.state.selSourceId];
            dataService.getSourceInfo(ss).then(
                function (response) {
                    $scope.ss = response.data;
                    console.dir($scope.ss);
                    $scope.state.selHarvestId = $scope.ss.type;
                    $scope.selectedIC = $.grep($scope.icConfigs, function (e) {
                        return e.name === $scope.state.selHarvestId;
                    })[0];
                    $scope.opMode = 'update';
                }
            );
        };
        $scope.ingestorTypeChanged = function () {
            console.log('ingestorTypeChanged:' + $scope.state.selHarvestId);
            var selICName = $scope.state.selHarvestId;
            $scope.selectedIC = $.grep($scope.icConfigs, function (e) {
                return e.name === selICName;
            })[0];
            console.dir($scope.selectedIC);
            prepInitialParams($scope);
        };
        function prepInitialParams($scope) {
            angular.forEach($scope.selectedIC.params, function (param) {
                $scope.ss.params[param.name] = '';
                if (param.default) {
                    $scope.ss.params[param.name] = param.default;
                }
            });
        };

        $scope.prepNewSource = function () {
            $scope.opMode = 'new';
            $scope.ss = {};
            $scope.ss.params = {};
            if ($scope.icConfigs) {
                $scope.state.selHarvestId = $scope.icConfigs[0].name;
                $scope.selectedIC = $scope.icConfigs[0];
            }
            prepInitialParams($scope);
        };
        $scope.saveSource = function () {
            var ss = $scope.ss;
            var payload = {};
            payload.opMode = $scope.opMode;
            payload.sourceID = $.trim(ss.sourceID);
            payload.sourceName = $.trim(ss.name);
            payload.dataSource = $.trim(ss.dataSource);
            payload.ingestMethod = $scope.state.selHarvestId;
            payload.params = {};
            angular.forEach(ss.params, function (value, key) {
                payload.params[key] = value;
            });
            dataService.saveSourceDesc(payload)
                .then(function (response) {
                    alert("Source description is successfully saved!");
                }, function (reason) {
                    console.dir(reason);
                    alert("An error occurred while saving source description:" + reason.data);
                });
        };
    }]);
foundryCtrls.controller('HelpController', ['$scope', '$uibModalInstance',
    function ($scope, $uibModalInstance) {
        $scope.ok = function () {
            $uibModalInstance.dismiss('cancel');
        };
    }
]);

foundryCtrls.controller('TransformationController', ['$scope', 'helpService',
    'dataService', 'sourceSummaryList',
    function ($scope, helpService, dataService, sourceSummaryList) {
        $scope.ssList = sourceSummaryList.data;
        $scope.state = {selectedJsonPath: '', selSourceId: -1};
        $scope.selectedSource = {};
        $scope.trScript = '';
        $scope.pkJsonPathFirst = '';
        $scope.aceLoaded = function (_editor) {
            $scope.aceSession = _editor.getSession();
        };


        $scope.showApplyHelp = function () {
            helpService.openApplyHelp();
        };
        $scope.showJSONTLHelp = function () {
            helpService.openJSONTLHelp();
        };

        $scope.rule2Builder = function () {
            var selection = $scope.aceSession.selection;
            var selRange = selection.getRange();
            var i, start;
            var selectedRule = '';
            if (selRange) {
                start = selRange.start.row;
                if (selRange.isMultiLine()) {
                    var x = $scope.aceSession.getLines(start, selRange.end.row);
                    for (i = 0; i < x.length; i++) {
                        selectedRule += x[i] + "\n";
                    }
                    console.dir(x);
                } else {
                    selectedRule = $scope.aceSession.getLine(start);
                }
            }
            console.dir(selRange);
            console.log("selectedRule:" + selectedRule);
            if (selectedRule) {
                var rule = foundry.JSONTLParsingModule.parseRule(selectedRule);
                if (rule) {
                    $scope.destPath = rule.destColumnName;
                    $scope.srcJsonPath1 = rule.sourceColumnNames[0];
                    $scope.srcJsonPaths = rule.sourceColumnNames;
                    if (rule.script) {
                        $scope.ruleScript = rule.script;
                    } else {
                        $scope.ruleScript = '';
                    }
                    $scope.ruleTrFunction = rule.trFunction ? rule.trFunction : '';
                    if ($scope.ruleTrFunction) {
                        $scope.ruleScript = $scope.ruleTrFunction;
                    }
                }
            }
        };

        $scope.showSelInSrcTree = function (idx) {
            var jsonPath = $.trim($scope.srcJsonPaths[idx]);
            if (jsonPath) {
                foundry.TreeModule.jsonPath2TreeNodeSelector(jsonPath, $('#jst1'));
            }
        };
        $scope.populateFromSrcTree = function (idx) {
            var selected = $.trim($scope.selectedSrcJsonPath);
            if (selected) {
                var existingJsonPath = $.trim($scope.srcJsonPaths[idx]);
                if (existingJsonPath) {
                    if (confirm('Do you want to overwrite existing source JSON path?')) {
                        $scope.srcJsonPaths[idx] = selected;
                        if (idx === 0) {
                            $scope.srcJsonPath1 = selected;
                        }
                    }
                } else {
                    $scope.srcJsonPaths[idx] = selected;
                    if (idx === 0) {
                        $scope.srcJsonPath1 = selected;
                    }
                }
            }
        };
        $scope.deleteRuleColumn = function (idx) {
            $scope.srcJsonPaths.splice(idx, 1);
        };
        $scope.addRuleColumn = function () {
            $scope.srcJsonPaths.push('');
        };

        function buildTrRule() {
            var i, first = true, sjp, s = "transform";
            var destPath = $.trim($scope.destPath);
            var srcJsonPath1, applyScript = $.trim($scope.ruleScript);
            if (!destPath) {
                return null;
            }
            if ($scope.srcJsonPaths.length > 1) {
                s += " columns ";
                for (i = 0; i < $scope.srcJsonPaths.length; i++) {
                    sjp = $.trim($scope.srcJsonPaths[i]);
                    if (!sjp) {
                        return null;
                    }
                    if (!first) {
                        s += ' , ';
                    }
                    s += '"' + sjp + '"';
                    first = false;
                }
                s += ' to "' + destPath + '"';
            } else {
                s += " column \"";
                srcJsonPath1 = $.trim($scope.srcJsonPath1);
                if (!srcJsonPath1) {
                    return null;
                }
                s += srcJsonPath1 + "\" to \"" + destPath + "\"";
            }
            if ($scope.ruleTrFunction) {
                s += " apply " + $scope.ruleTrFunction;
            } else if (applyScript) {
                if (/toStandardDate|toStandardTime|toStandardDateTime/.test(applyScript)) {
                    s += " apply " + applyScript;
                } else {
                    s += " apply {{ " + $scope.ruleScript + " }}";
                }
            }
            s += ";";
            return s;
        }

        $scope.testRule = function () {
            if (!$scope.srcJsonPaths) {
                return;
            }
            var transformationRule = buildTrRule();
            console.log("transformationRule:" + transformationRule);
            dataService.testRule(transformationRule, JSON.stringify($scope.sampleSourceData))
                .then(function (response) {
                    $scope.destData = response.data.ruleTrTree;
                }, function (reason) {
                    console.dir(reason);
                    var msg = "An error occurred while testing the transformation script ";
                    if (reason.data) {
                        msg += ': ' + reason.data;
                    }
                    alert(msg);
                });
        };

        $scope.hasBuiltRule = function () {
            return $.trim($scope.destPath) && $scope.srcJsonPaths && $.trim($scope.srcJsonPath1);
        };

        $scope.append2TrScript = function () {
            if ($scope.aceSession && $scope.hasBuiltRule()) {
                var s = buildTrRule();
                if (s) {
                    $scope.aceSession.insert({row: $scope.aceSession.getLength(), column: 0}, "\n" + s);
                }
            }
        };
        $scope.hasSelectedSource = function () {
            return $scope.selectedSource && !$.isEmptyObject($scope.selectedSource);
        }
        $scope.setSelectedJsonPath = function (selJsonPath) {
            $scope.$apply(function () {
                $scope.state.selectedJsonPath = selJsonPath;
            });
            console.log('state.selectedJsonPath:' + $scope.state.selectedJsonPath);
        };
        $scope.sourceChanged = function () {
            console.log('sourceChanged:' + $scope.state.selSourceId);
            var ss = $scope.ssList[$scope.state.selSourceId];
            $scope.selectedSource = ss;
            console.dir($scope.selectedSource);
            $scope.trScript = ss.transformScript;
            if (ss.primaryKeyJSONPath) {
                $scope.pkJsonPathFirst = ss.primaryKeyJSONPath[0];
            }
            $scope.sourceData = null;
            $scope.destData = null;
            $scope.srcJsonPath1 = null;
            $scope.srcJsonPaths = [];
            $scope.destPath = null;
            $scope.ruleScript = null;
        };
        $scope.deletePKJsonPath = function (idx) {
            $scope.selectedSource.primaryKeyJSONPath.splice(idx, 1);
        };
        $scope.addPKJsonPath = function () {
            $scope.selectedSource.primaryKeyJSONPath.push('');
        };
        $scope.sample = function () {
            dataService.sample($scope.selectedSource)
                .then(function (response) {
                        $scope.sourceData = response.data.sampleTree;
                        $scope.sampleSourceData = response.data.sample;
                    },
                    function (reason) {
                        console.dir(reason);
                        alert("Cannot sample data:" + reason);
                    });
        };
        $scope.testTransformation = function () {
            var ss = $scope.selectedSource;
            dataService.testTransformation(ss.sourceID, ss.dataSource, $scope.trScript)
                .then(function (response) {
                    $scope.trResultJson = JSON.stringify(response.data, null, 2);
                }, function (reason) {
                    console.dir(reason);
                    alert("An error occurred while testing the transformation script:" + reason);
                });
        };

        $scope.updateSource = function () {
            var i, v, payload = {}, ss = $scope.selectedSource;
            if (!ss || $.isEmptyObject(ss)) {
                return;
            }
            payload.sourceID = ss.sourceID;
            payload.dataSource = ss.dataSource;
            payload.transformScript = $scope.trScript;
            v = $.trim($scope.pkJsonPathFirst);
            payload.pkJsonPath = [];

            if (!payload.transformScript || !v) {
                alert("Both the transformation script and primary key json path are required!");
                return;
            }
            payload.pkJsonPath.push(v);
            for (i = 1; i < $scope.primaryKeyJSONPath.length; ++i) {
                payload / pkJsonPath.push($.trim($scope.primaryKeyJSONPath[i]));
            }
            dataService.updateSourceDesc(payload)
                .then(function (response) {
                    alert("Both transformation script and primary key definition are successfully saved!");
                }, function (reason) {
                    console.dir(reason);
                    alert("An error occurred while saving the transformation script and primary key:" + reason);
                });
        };
    }])
;
foundryCtrls.controller('DashboardController', ['$scope', 'dataService', 'sourceProcStatsList',
    function ($scope, dataService, sourceProcStatsList) {
        $scope.spsList = sourceProcStatsList.data;
        $scope.getStatsData = function () {
            var i, sdList = [], sds, row, bi;
            var statusMap = ['Not Started', 'In Process', 'Finished'];
            for (i = 0; i < $scope.spsList.length; i++) {
                sds = $scope.spsList[i];
                row = {};
                row.Source = sds.name + ' (' + sds.sourceID + ')';
                row.Total = sds.total;
                row.Finished = sds.statusCounts.finished ? sds.statusCounts.finished : 0;
                row.Error = sds.statusCounts.error ? sds.statusCounts.error : 0;
                row.ingestStart = '';
                row.ingestEnd = '';
                row.status = '';
                if (sds.batchInfo) {
                    bi = sds.batchInfo;
                    row.ingestStart = bi.ingestionStartDatetime ? bi.ingestionStartDatetime : '';
                    row.ingestEnd = bi.ingestionEndDatetime ? bi.ingestionEndDatetime : '';
                    row.status = bi.ingestionStatus ? statusMap[parseInt(bi.ingestionStatus)] : '';
                }
                sdList.push(row);
            }
            return sdList;
        };
        $scope.rowList = $scope.getStatsData();

    }]);

foundryCtrls.controller('TransformController', ['$scope', 'dataService',
    function ($scope, dataService) {
        $scope.apiKey = '';
        $scope.trScript = '';
        $scope.sampleData = '';
        $scope.transform = function () {
            dataService.transform($scope.apiKey, $scope.trScript, $scope.sampleData)
                .then(function (response) {
                        $scope.trResultJson = JSON.stringify(response.data, null, 2);
                    }, function (reason) {
                        console.dir(reason);
                        var msg = "An error occurred while testing the transformation script ";
                        if (reason.data) {
                            msg += ': ' + reason.data;
                        }
                        alert(msg);
                    }
                );
        };
    }]);
