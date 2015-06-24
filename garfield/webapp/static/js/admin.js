
var app = angular.module('dsense', []);

app.controller('mainController', ['$scope', '$http', function($scope, $http){

	$scope.topiclist = [];
	$scope.error = "";

	$scope.refresh = function() {
		$http.get("/rest?modelinfo=all").success(function(data) {
			if (data['results'].length != 0) {
				$scope.topiclist = data['results']
			}

		}).error(function(){
			console.err("Refresh failed");
		});
	};

	//setInterval($scope.queryrealtime, 2000);

	$scope.refresh()

}]);

var scope;
$(document).ready(function(){
	scope = angular.element('[ng-controller=mainController]').scope();
});

