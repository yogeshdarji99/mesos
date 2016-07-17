// Load the Visualization API and the corechart package.
google.charts.load('current', {'packages':['corechart']});

// Set a callback to run when the Google Visualization API is loaded.
google.charts.setOnLoadCallback(drawChart);

// Callback that creates and populates a data table,
// instantiates the pie chart, passes in the data and
// draws it.
function drawChart() {
  var data = google.visualization.arrayToDataTable([
    ['Release', 'IBM', 'Metamarkets', 'Twitter', 'Mesosphere', 'Other', 'Microsoft', 'Apple', {'role': 'annotation'}],
    ['0.24', 5  , 0, 154, 198, 83 , 13 , 0 , ''],
    ['0.25', 29 , 0, 76 , 147, 54 , 13 , 0 , ''],
    ['0.26', 20 , 0, 48 , 181, 91 , 60 , 0 , ''],
    ['0.27', 24 , 0, 22 , 540, 163, 25 , 0 , ''],
    ['0.28', 19 , 0, 0  , 385, 55 , 48 , 0 , ''],
    ['HEAD', 210, 2, 0  , 995, 328, 123, 15, '']
  ]);

  // Set chart options
  var options = {
    'title': 'Commit Breakdown by Organizations and Releases',
    'width': 600,
    'height': 400,
    'isStacked': true,
    'groupWidth': '90%',
    'legend': { position: 'top', maxLines: 30 }
  };

  // Instantiate and draw our chart, passing in some options.
  var chart = new google.visualization.ColumnChart(
      document.getElementById('commit_breakdown_by_organizations_and_releases'));
  chart.draw(data, options);
  drawContributorsChart();
}

function drawContributorsChart() {
  var data = google.visualization.arrayToDataTable([
    ['Release', 'Committers', 'Contributors', {'role': 'annotation'}],
    ['0.24', 208, 245 , ''],
    ['0.25', 139, 180 , ''],
    ['0.26', 97 , 303 , ''],
    ['0.27', 214, 560 , ''],
    ['0.28', 126, 381 , ''],
    ['HEAD', 409, 1264, '']
  ])

  // Set chart options
  var options = {
    'title': 'Commit Breakdown by Contributors and Committers',
    'width': 600,
    'height': 400,
    'isStacked': true,
    'groupWidth': '90%',
    'legend': { position: 'top', maxLines: 30 }
  };

  // Instantiate and draw our chart, passing in some options.
  var chart = new google.visualization.ColumnChart(
      document.getElementById('commit_breakdown_by_contributors_and_committers'));
  chart.draw(data, options);
  drawUniqueContributorsChart();
}

function drawUniqueContributorsChart() {
  var data = google.visualization.arrayToDataTable([
    ['Release', 'Number of Contributors', 'Number of Committers', {'role': 'annotation'}],
    ['0.24', 40, 14, ''],
    ['0.25', 37, 13, ''],
    ['0.26', 42, 14, ''],
    ['0.27', 51, 13, ''],
    ['0.28', 41, 10, ''],
    ['HEAD', 91, 16, '']
  ])

  // Set chart options
  var options = {
    'title': 'Number of Contributors and Committers per Release',
    'width': 600,
    'height': 400,
    'isStacked': true,
    'groupWidth': '90%',
    'legend': { position: 'top', maxLines: 30 }
  };

  // Instantiate and draw our chart, passing in some options.
  var chart = new google.visualization.ColumnChart(
      document.getElementById('number_of_contributors_and_committers_per_release'));
  chart.draw(data, options);
  drawMonthlyChart()
}

function drawMonthlyChart() {
  var data = google.visualization.arrayToDataTable([
    ['Release', 'Apple', 'Metamarkets', 'Twitter', 'Mesosphere', 'Other', 'Microsoft', 'IBM', {'role': 'annotation'}],
    ['2015-07', 0, 0, 71 , 124, 45 , 8 , 1 , ''],
    ['2015-08', 2, 0, 107, 99 , 46 , 5 , 7 , ''],
    ['2015-09', 0, 0, 71 , 132, 56 , 30, 28, ''],
    ['2015-10', 0, 0, 17 , 94 , 29 , 43, 10, ''],
    ['2015-11', 1, 0, 31 , 138, 71 , 0 , 9 , ''],
    ['2015-12', 0, 0, 1  , 195, 61 , 17, 10, ''],
    ['2016-01', 0, 0, 0  , 301, 71 , 22, 13, ''],
    ['2016-02', 0, 0, 0  , 258, 46 , 12, 12, ''],
    ['2016-03', 0, 0, 0  , 296, 78 , 42, 36, ''],
    ['2016-04', 2, 0, 0  , 184, 75 , 37, 58, ''],
    ['2016-05', 6, 0, 0  , 237, 50 , 50, 52, ''],
    ['2016-06', 7, 1, 0  , 268, 118, 16, 66, ''],
    ['2016-07', 1, 1, 0  , 117, 26 , 0 , 7 , '']
  ])

  // Set chart options
  var options = {
    'title': 'Commit Breakdown by Organizations and Months',
    'width': 600,
    'height': 400,
    'isStacked': true,
    'groupWidth': '90%',
    'legend': { position: 'top', maxLines: 30 }
  };

  // Instantiate and draw our chart, passing in some options.
  var chart = new google.visualization.ColumnChart(
      document.getElementById('commit_breakdown_by_organizations_and_months'));
  chart.draw(data, options);
}
