Chart.defaults.global.animation.duration = 2000; // Animation duration
Chart.defaults.global.responsiveAnimationDuration = 2000; // Animation duration
Chart.defaults.global.title.display = false; // Remove title
Chart.defaults.global.title.text = "Chart"; // Title
Chart.defaults.global.title.position = 'bottom'; // Title position
Chart.defaults.global.defaultFontColor = '#999'; // Font color
Chart.defaults.global.defaultFontSize = 10; // Font size for every label

// Chart.defaults.global.tooltips.backgroundColor = '#FFF'; // Tooltips background color
Chart.defaults.global.tooltips.borderColor = 'white'; // Tooltips border color
Chart.defaults.global.legend.labels.padding = 0;
Chart.defaults.scale.ticks.beginAtZero = true;
Chart.defaults.scale.gridLines.zeroLineColor = 'rgba(255, 255, 255, 0.1)';
Chart.defaults.scale.gridLines.color = 'rgba(255, 255, 255, 0.02)';

Chart.defaults.global.legend.display = false;
var Chart2 = document.getElementById('myChart2').getContext('2d');

var chart = new Chart(Chart2, {
type: 'bar',
data: {
  labels: ["January", "February", "March", "April", "May", "June", "July", "August", "September", "November", "December"],
  datasets: [{
    label: "Month",
    fill: false,
    lineTension: .4,
    startAngle: 2,
    data: [],
    // , '#ff6384', '#4bc0c0', '#ffcd56', '#457ba1'
    backgroundColor: "transparent",
    pointBorderColor: "#ffcd56",
    borderColor: '#ffcd56',
    borderWidth: 2,
    showLine: true,
  }]
},

// Configuration options
options: {
  title: {
    display: false,
    maintainAspectRatio: false
  }
}
});


console.log(Chart.defaults.global);