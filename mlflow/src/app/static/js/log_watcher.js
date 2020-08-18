/**
(c) Copyright 2019, Splice Machine Inc.
All Rights Reserved
**/

function watchLogs(task_id){
    setInterval(function(){
        var data = JSON.stringify({"task_id": task_id});

    var xhr = new XMLHttpRequest();
    xhr.withCredentials = true;
    xhr.responseType = 'json';

    xhr.onload = function(e) {
      if (this.status == 200) {
        document.getElementById("logs").innerHTML = JSON.parse(xhr.responseText)['logs'].join('\n');
      } else {
        document.getElementById("logs").innerHTML = "Error Retrieving Logs.";
      }
    };

    xhr.open("POST", "/api/ui/logs");
    xhr.setRequestHeader("Content-Type", "application/json");

    xhr.send(data);
    }, 2000)
}