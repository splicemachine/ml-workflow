/**
* Export Tracker table in the JSON Format
**/
function exportJSON(){
    $('#grid-data').tableExport({type:'json'});
}

/**
* Export Tracker table in the TXT format
**/
function exportTXT(){
    $('#grid-data').tableExport({type:'txt'});
}

/**
* Export Tracker table in the CSV Format
**/
function exportCSV(){
    $('#grid-data').tableExport({type:'csv'});
}