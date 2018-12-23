const DATATABLE_SELECTOR = "#top-tweets-table";

// https://stackoverflow.com/questions/5180382/convert-json-data-to-a-html-table,
// by https://stackoverflow.com/users/1220550/peter-b
// Builds the HTML Table out of myList.
function buildHtmlTable(data, selector) {
  var tHead$ = $('<thead/>');
  var columns = addAllColumnHeaders(data, selector, tHead$);
  var tBody$ = $('<tbody />');
  for (var i = 0; i < data.length; i++) {
    var row$ = $('<tr/>');
    for (var colIndex = 0; colIndex < columns.length; colIndex++) {
      var cellValue = data[i][columns[colIndex]];
      if (cellValue == null) cellValue = "";
      row$.append($('<td/>').html(cellValue));
    }
    tBody$.append(row$);
  }

  $(selector).append(tBody$);

  var tFoot$ = $('<tfoot/>');
  addAllColumnHeaders(data, selector, tFoot$);
}

// Adds a header row to the table and returns the set of columns.
// Need to do union of keys from all records as some records may not contain
// all records.
function addAllColumnHeaders(data, selector, section_elem) {
  var columnSet = [];
  var headerTr$ = $('<tr/>');

  for (var i = 0; i < data.length; i++) {
    var rowHash = data[i];
    for (var key in rowHash) {
      if ($.inArray(key, columnSet) == -1) {
        columnSet.push(key);
        headerTr$.append($('<th/>').html(key));
      }
    }
  }
  section_elem.append(headerTr$);
  $(selector).append(section_elem);

  return columnSet;
}

$(function() {
    $.getJSON(BASE_API_URL + "/get_tweets/50/retweet_count/desc", function(data) {
      for (let i = 0; i < data.length; i++) {
        data[i].text = "<a href=\"https://twitter.com/statuses/" + data[i].row_id + "\" target=\"_blank\">" + data[i].text + "</a>";
        delete data[i].row_id;
      }
      buildHtmlTable(data, DATATABLE_SELECTOR);
      $( DATATABLE_SELECTOR ).DataTable( {
        "order": [[ 4, "desc" ]]
      } );
    });
});