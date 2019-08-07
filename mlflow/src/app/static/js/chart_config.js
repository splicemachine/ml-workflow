Chart.defaults.global.animation.duration = 2000; // Animation duration
Chart.defaults.global.responsiveAnimationDuration = 2000; // Animation duration
Chart.defaults.global.title.display = false; // Remove title
Chart.defaults.global.title.text = "Chart"; // Title
Chart.defaults.global.title.position = 'bottom'; // Title position
Chart.defaults.global.defaultFontColor = '#999'; // Font color
Chart.defaults.global.defaultFontSize = 10; // Font size for every label

// Chart.defaults.global.tooltips.backgroundColor = '#FFF'; // Tooltips background color
Chart.defaults.global.tooltips.borderColor = 'white'; // Tooltips border color
Chart.defaults.scale.ticks.beginAtZero = true;
Chart.defaults.scale.gridLines.zeroLineColor = 'rgba(255, 255, 255, 0.1)';
Chart.defaults.scale.gridLines.color = 'rgba(255, 255, 255, 0.02)';

Chart.defaults.global.legend.display = true;
var Chart2 = document.getElementById('myChart2').getContext('2d');

var chart = new Chart(Chart2, {
type: 'bar',
data: {
  labels: ["January", "February", "March", "April", "May", "June", "July", "August", "September", "November", "December"],
  datasets: []
},

// Configuration options
options: {
  title: {
    display: false,
    maintainAspectRatio: false
  },
  scales: {
    xAxes: [{ stacked: true }],
    yAxes: [{ stacked: true }]
  }
}
});


console.log(Chart.defaults.global);