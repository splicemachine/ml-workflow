/**
(c) Copyright 2019, Splice Machine Inc.
All Rights Reserved
**/
function escapeHtml(unsafe) { // prevent XSS
    return unsafe
         .replace(/&/g, "&amp;")
         .replace(/</g, "&lt;")
         .replace(/>/g, "&gt;")
         .replace(/"/g, "&quot;")
         .replace(/'/g, "&#039;");
}

function watchLogs(task_id){
    setInterval(function(){
        var data = JSON.stringify({"task_id": task_id});

    var xhr = new XMLHttpRequest();
    xhr.withCredentials = true;

    xhr.onload = function(e) {
      if (this.status == 200) {
        document.getElementById("logs").innerHTML = escapeHtml(JSON.parse(xhr.responseText)['logs'].join('\n'));
      } else {
        document.getElementById("logs").innerHTML = "Error Retrieving Logs.";
      }
    };

    xhr.open("POST", "/api/ui/logs");
    xhr.setRequestHeader("Content-Type", "application/json");

    xhr.send(data);
    }, 100)
}