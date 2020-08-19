/**
(c) Copyright 2020, Splice Machine Inc.
All Rights Reserved
**/

function toggleParameter(name){
    var service = document.getElementById(name);
    if (service == null){
        document.getElementById("services").innerHTML += `<input type='hidden' name='${name}' id='${name}' value=true>`;
    } else {
        service.parentNode.removeChild(service);
    }
}

function toggleDatabaseCreate(){
    var elem = document.getElementById('input_create_model_table');
    if (elem.value == "true"){
        document.getElementById("create_table_inputs").style.display = "block";
    } else {
        document.getElementById("create_table_inputs").style.display = "none";
    }
}
