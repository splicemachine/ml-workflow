/**
(c) Copyright 2019, Splice Machine Inc.
All Rights Reserved
**/
var WIDGET_SIZE = 'col-md-6'; // bootstrap div size for access widgets
var chart_colors = ['#18466f', '#22629b', '#2c7ec7', '#318dde', '#5aa3e4', '#83baeb', '#acd1f1', '#d5e8f8']
/**
Cache API UI responses to SessionStorage
to avoid excessive db calls
**/
function setGlobalJobData(chartData, total) {
    sessionStorage.setItem('chart', JSON.stringify(chartData));
    sessionStorage.setItem('total', JSON.stringify(total));
}

/**
* Setup Chart Widget
**/
function setupChart(chart_data){
    chart.data.datasets = [];
    var color_idx = 0;
    var max_color_idx = chart_colors.length - 1;

    for (var user in chart_data){
        chart.data.datasets.push(
            {
                label: user,
                data: chart_data[user],
                backgroundColor: chart_colors[color_idx],
                fillColor: 'rgba(220,220,220,0)',
                hoverBackgroundColor: chart_colors[color_idx],
                hoverBorderWidth: 0
            }
        );
        if (color_idx == max_color_idx){
            color_idx = 0;
        } else {
            color_idx++;
        }
    }

   chart.update();
}
/**
Get monthly aggregated
job data, along with the total
jobs executed for use in the UI
**/
function getJobData(force) {
    console.log(force);
    if (force || (sessionStorage.getItem('chart') === null ||
            sessionStorage.getItem('total') === null)) {
        document.getElementById('refresh-btn').innerHTML = 'Loading...';
        var xhr = new XMLHttpRequest();
        xhr.open('GET', '/api/ui/get_monthly_aggregated_jobs', true);
        xhr.onload = function() {
            if (xhr.status != 200) {
                alert(`Error ${xhr.status}: ${xhr.statusText}`);
                document.getElementById('refresh-btn').innerHTML = 'Failed.'
            } else {
                var json_response = JSON.parse(this.responseText);
                console.log(json_response);
                setGlobalJobData(json_response.data, json_response.total);
                setupChart(json_response.data);
                document.getElementById('job_num').innerHTML = json_response.total;
                document.getElementById('refresh-btn').innerHTML = 'Refresh'
            }

        }
        xhr.send();
    } else {
        setupChart(JSON.parse(sessionStorage.getItem('chart')));
        document.getElementById('job_num').innerHTML = JSON.parse(sessionStorage.getItem('total'));
    }
}

/**
Cache API UI responses to SessionStorage
to avoid excessive db calls
**/
function setGlobalHandlerData(count, html) {
    sessionStorage.setItem('enabled_handler_count', JSON.stringify(count));
    sessionStorage.setItem('widget_html', JSON.stringify(html));
}

function createAccessWidgetHTML(enabled, widget_name) {
    var icon_class;
    var status;
    if (enabled) {
        icon_class = "fa fa-check fa-fw succ";
        status = 'ENABLED';
    } else {
       icon_class = "fa fa-times fa-fw dang";
       status = 'DISABLED';
    }
    var html= `<div class="${WIDGET_SIZE}">
             <div class="box">
                 <i id="${widget_name}_ICON" class="${icon_class}"></i>
                 <div class="info">
                     <h3>${widget_name.replace('_', ' ')}</h3>
                      <p style="margin-top:0" id="${widget_name}_STATUS">${status}</p>
                  </div>
             </div>
            </div>`;
     console.log(html);
     return html;
}

/**
* Get Access Handlers (cached from index if that was visited yet).
* This function retrieves and caches a) the number of
* enabled services, and b) an HTML string representing the widgets
* in the Access GUI. We do this in one function since the same HTTP endpoint
* response can be parsed to calculate both of these products
**/
function getHandlerWidgetData(execution_function){
        document.getElementById('refresh-btn').innerHTML = 'Loading...';
        var xhr = new XMLHttpRequest();
        xhr.open('GET', '/api/ui/get_handler_data', true);
        xhr.onload = function() {
            if (xhr.status != 200) {
                alert(`Error ${xhr.status}: ${xhr.statusText}`);
                document.getElementById('refresh-btn').innerHTML = 'Failed'
                return;
            } else {
                var json_response = JSON.parse(this.responseText);
                var results = json_response.data;
                var row_switch = 0;
                var count = 0;
                var html_string = "";
                for (var i = 0; i < results.length; i++) {
                    count += results[i][1];

                    var enabled = results[i][1] == 1;
                    var name = results[i][0];
                    if (row_switch == 0){
                        html_string += "<div class='row'>" + createAccessWidgetHTML(enabled, name);
                        row_switch++;
                    } else if (row_switch == 1) {
                        html_string += createAccessWidgetHTML(enabled, name) + "</div>";
                        row_switch--;
                    }
                }
                if (row_switch == 1){
                    html_string += "</div>";
                } // odd number of widgets

                setGlobalHandlerData(count, html_string);
                execution_function(count, html_string);
                document.getElementById('refresh-btn').innerHTML = 'Refresh'

        }
     }
     xhr.send();
}


function getAccessHandlerData(force) {
    console.log(force);
    if (force || (sessionStorage.getItem('enabled_handler_count') === null ||
           sessionStorage.getItem('widget_html') == null)){
        getHandlerWidgetData(function(count, html_string){ // callback when loading happens
            document.getElementById('widgets').innerHTML = html_string // html @ 1
        }) // run after async xhr
    } else {
        var widget_html = JSON.parse(sessionStorage.getItem('widget_html'));
        console.log(widget_html);
        document.getElementById('widgets').innerHTML = widget_html;
    }

}
/**
Get enabled handlers count
**/
function getHomeHandlerData(force) {
    console.log(force)
    if (force || (sessionStorage.getItem('enabled_handler_count') === null ||
           sessionStorage.getItem('widget_html') == null)){
        getHandlerWidgetData(function(count, html_string){ // callback when loading happens
           document.getElementById('handler_num').innerHTML = count; // html @ 1
        }); // execution function to run on xhr onload
    } else {
        var count = JSON.parse(sessionStorage.getItem('enabled_handler_count'));
        console.log(count);
        document.getElementById('handler_num').innerHTML = count;
    }
}

function refreshHome(force) {
    setTimeout(function() {
        getJobData(force);
    }, 0);
    setTimeout(function(){
        getHomeHandlerData(force);
    }, 0); /** asynchronous **/
}

function refreshAccess(force) {
    setTimeout(function() {
        getAccessHandlerData(force);
    }, 0);
}