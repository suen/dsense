
var app = angular.module('dsense', []);

app.controller('mainController', ['$scope', '$http', function($scope, $http){

	$scope.resultset = []
	$scope.query = ""
	$scope.queryInProcess = false;

    $scope.submitQuery = function(){
		
		if ($scope.query == "")
			return;

		param = { query : $scope.query }

		$scope.queryInProcess = true 

		$scope.resultset = []
        $http.get("/rest", {params: param } ).success(function(data){ 
			console.log("GET query="+ $scope.query) 
			console.log("Result : " + data)
			$scope.resultset = data['results']
			$scope.queryInProcess = false
		}).error(function() {
			console.err("GET failed")
		});
    };

}]);

var scope;
$(document).ready(function(){
	scope = angular.element('[ng-controller=mainController]').scope();
});

