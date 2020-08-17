/**
(c) Copyright 2020, Splice Machine Inc.
All Rights Reserved
**/

function toggleParameter(name){
    var service = document.getElementById(name);
    if (service == null){
        document.getElementById("services") += `<input type='hidden' name=${name} value=true>`;
    } else {
        service.parentNode.removeChild(service);
    }
}

function toggleDatabaseCreate(){
    if (document.getElementById('input_create_model_table').value) == "true"){
        document.getElementById("create_table_inputs").style.display = "block";
    } else {
        document.getElementById("create_table_inputs").style.display = "none";
}
