/**
Cache API UI responses to SessionStorage
to avoid excessive db calls
**/
function setGlobalJobData(chartData, total){
    sessionStorage.setItem('chart', JSON.stringify(chartData));
    sessionStorage.setItem('total', JSON.stringify(total));
}

/**
Get monthly aggregated
job data, along with the total
jobs executed for use in the UI
**/
function getJobData(force){
    console.log(force);
    if (force || (sessionStorage.getItem('chart') === null ||
            sessionStorage.getItem('total') === null)){
        var xhr = new XMLHttpRequest();
        xhr.open('GET', '/api/ui/get_monthly_aggregated_jobs', true);
        xhr.onload = function(){
            if (xhr.status != 200){
                alert(`Error ${xhr.status}: ${xhr.statusText}`);
            } else {
                var json_response = JSON.parse(this.responseText);
                console.log(json_response);
                setGlobalJobData(json_response.data, json_response.total);
                chart.data.datasets[0].data = json_response.data;
                chart.update();
                document.getElementById('job_num').innerHTML = json_response.total;
            }

        }
        xhr.send();
    } else {
        chart.data.datasets[0].data = JSON.parse(sessionStorage.getItem('chart'));
        chart.update();
        document.getElementById('job_num').innerHTML = JSON.parse(sessionStorage.getItem('total'));
    }
}

/**
Cache API UI responses to SessionStorage
to avoid excessive db calls
**/
function setGlobalHandlerData(enabled_handlers){
    sessionStorage.setItem('enabled_handlers', JSON.stringify(enabled_handlers));
}

/**
Get enabled handlers count
**/
function getHandlerData(force){
    console.log(force)
    if (force || sessionStorage.getItem('enabled_handlers') === null){

        var xhr = new XMLHttpRequest();
        xhr.open('GET', '/api/ui/get_enabled_handler_count', true);
        xhr.onload = function(){
            if (xhr.status != 200){
                alert(`Error ${xhr.status}: ${xhr.statusText}`);
            } else {
                var json_response = JSON.parse(this.responseText);
                console.log(json_response);
                setGlobalHandlerData(json_response.count);
                document.getElementById('handler_num').innerHTML = json_response.count;
            }

        }
        xhr.send();
    }else {
       document.getElementById('handler_num').innerHTML = JSON.parse(
            sessionStorage.getItem('enabled_handlers')
       );
    }
}

function refresh(force){
    document.getElementById('loading').innerHTML = "Loading Widgets..."
    getJobData(force);
    getHandlerData(force);
    document.getElementById('loading').innerHTML = "";
}