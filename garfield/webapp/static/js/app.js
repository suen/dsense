
var app = angular.module('dsense', []);

app.controller('mainController', ['$scope', '$http', function($scope, $http){

	$scope.resultset = [];
	$scope.topwords = [];
	$scope.query = "";
	$scope.stream = [];
	$scope.queryInProcess = false;
	$scope.noresult = false;
	$scope.executedquery = "";
	$scope.error = "";
	$scope.uptime = "0s";

	$scope.tweetcount = 0;
	$scope.tweetrate = "NaN"; 

    $scope.submitQuery = function(){
		
		$scope.firstrun = false;
		$scope.executedquery = "";
		
		if ($scope.query == "")
			return;

		param = { query : $scope.query }

		$scope.queryInProcess = true ;

		$scope.resultset = [];
        $http.get("/rest", {params: param } ).success(function(data){ 
			console.log("GET query="+ $scope.query);
			console.log(data);
			
			$scope.resultset = data['results'];
			$scope.queryInProcess = false;

			if (data['results'].length == 0)
				$scope.noresult = true;
			else
				$scope.noresult = false;

			$scope.executedquery = $scope.query;
				
		}).error(function() {
			console.err("GET failed")
			$scope.queryInProcess = false;
			$scope.executedquery = $scope.query;
			$scope.error = "Server request failed";
		});
    };

	$scope.queryrealtime = function() {
		$http.get("/realtime").success(function(data) {
			if (data['results'].length != 0) {
				$scope.stream = data['results']
			}
			$scope.topwords = data['topwords'];

			$scope.tweetcount = data['counter'];
			$scope.tweetrate = data['rate'].toString() + " tweets/second";
		
			hour = data['uptime'] / (3600);
			hour = Math.floor(hour,0);
			min = data['uptime'] - (hour * 60);
			min = Math.floor(min/60,0);
			sec = data['uptime'] - (hour * 60) - (min*60);

			sec = Math.floor(sec,0);
			
			$scope.uptime = hour.toString() + "h" + min.toString() + "m" + sec.toString() + "s";

			//console.log(data)
		}).error(function(){
			console.err("Realtime stream failed");
		});
	};

	setInterval($scope.queryrealtime, 2000);

}]);

var scope;
$(document).ready(function(){
	scope = angular.element('[ng-controller=mainController]').scope();
});

