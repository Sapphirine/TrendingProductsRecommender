function wrap_li(input) {
  return "<li class=\"tpr-list-item\">" + input + "</li>"
}

function wrap_ul(input) {
  return "<ul class=\"tpr-list-basic\">" + input + "</ul>"
}

      /*
############# BAR CHART ###################
-------------------------------------------
*/

function dsBarChartBasics() {

    var margin = {top: 30, right: 5, bottom: 20, left: 50},
      width = 500 - margin.left - margin.right,
      height = 250 - margin.top - margin.bottom,
      colorBar = d3.scale.category20(),
      barPadding = 1;

    return {
      margin : margin,
      width : width,
      height : height,
      colorBar : colorBar,
      barPadding : barPadding
    };
}

function dsBarChart(target_selector, data, title) {

  var firstDatasetBarChart = data;

  var basics = dsBarChartBasics();

  var margin = basics.margin,
    width = basics.width,
     height = basics.height,
    colorBar = basics.colorBar,
    barPadding = basics.barPadding
    ;

  var   xScale = d3.scale.linear()
            .domain([0, firstDatasetBarChart.length])
            .range([0, width])
            ;

  // Create linear y scale
  // Purpose: No matter what the data is, the bar should fit into the svg area; bars should not
  // get higher than the svg height. Hence incoming data needs to be scaled to fit into the svg area.
  var yScale = d3.scale.linear()
      // use the max funtion to derive end point of the domain (max value of the dataset)
      // do not use the min value of the dataset as min of the domain as otherwise you will not see the first bar
       .domain([0, d3.max(firstDatasetBarChart, function(d) { return d.measure; })])
       // As coordinates are always defined from the top left corner, the y position of the bar
       // is the svg height minus the data value. So you basically draw the bar starting from the top.
       // To have the y position calculated by the range function
       .range([height, 0])
       ;

  //Create SVG element

  var svg = d3.select(target_selector)
      .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .attr("id","barChartPlot")
        ;

  var plot = svg
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
        ;

  plot.selectAll("rect")
       .data(firstDatasetBarChart)
       .enter()
       .append("rect")
      .attr("x", function(d, i) {
          return xScale(i);
      })
       .attr("width", width / firstDatasetBarChart.length - barPadding)
      .attr("y", function(d) {
          return yScale(d.measure);
      })
      .attr("height", function(d) {
          return height-yScale(d.measure);
      })
      .attr("fill", "lightgrey")
      ;


  // Add y labels to plot

  plot.selectAll("text")
  .data(firstDatasetBarChart)
  .enter()
  .append("text")
  .text(function(d) {
      return formatAsInteger(d3.round(d.measure));
  })
  .attr("text-anchor", "middle")
  // Set x position to the left edge of each bar plus half the bar width
  .attr("x", function(d, i) {
      return (i * (width / firstDatasetBarChart.length)) + ((width / firstDatasetBarChart.length - barPadding) / 2);
  })
  .attr("y", function(d) {
      return yScale(d.measure) - 5;
  })
  .attr("class", "yAxis")
  /* moved to CSS
  .attr("font-family", "sans-serif")
  .attr("font-size", "11px")
  .attr("fill", "white")
  */
  ;

  // Add x labels to chart

  var xLabels = svg
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + (margin.top + height)  + ")")
        ;

  xLabels.selectAll("text.xAxis")
      .data(firstDatasetBarChart)
      .enter()
      .append("text")
      .text(function(d) { return d.category;})
      .attr("text-anchor", "middle")
      // Set x position to the left edge of each bar plus half the bar width
               .attr("x", function(d, i) {
                  return (i * (width / firstDatasetBarChart.length)) + ((width / firstDatasetBarChart.length - barPadding) / 2);
               })
      .attr("y", 15)
      .attr("class", "xAxis")
      // .attr("transform", "rotate(90)")
      .attr("style", "font-size: 9; font-family: Helvetica, sans-serif")
      ;

  // Title

  svg.append("text")
    .attr("x", (width + margin.left + margin.right)/2)
    .attr("y", 15)
    .attr("class","title")
    .attr("text-anchor", "middle")
    .text(title)
    ;
}

 /* ** UPDATE CHART ** */

/* updates bar chart on request */

function updateBarChart(group, colorChosen) {

    var currentDatasetBarChart = datasetBarChosen(group);

    var basics = dsBarChartBasics();

    var margin = basics.margin,
      width = basics.width,
       height = basics.height,
      colorBar = basics.colorBar,
      barPadding = basics.barPadding
      ;

    var   xScale = d3.scale.linear()
      .domain([0, currentDatasetBarChart.length])
      .range([0, width])
      ;


    var yScale = d3.scale.linear()
        .domain([0, d3.max(currentDatasetBarChart, function(d) { return d.measure; })])
        .range([height,0])
        ;

     var svg = d3.select("#barChart svg");

     var plot = d3.select("#barChartPlot")
      .datum(currentDatasetBarChart)
       ;

        /* Note that here we only have to select the elements - no more appending! */
      plot.selectAll("rect")
        .data(currentDatasetBarChart)
        .transition()
      .duration(750)
      .attr("x", function(d, i) {
          return xScale(i);
      })
       .attr("width", width / currentDatasetBarChart.length - barPadding)
      .attr("y", function(d) {
          return yScale(d.measure);
      })
      .attr("height", function(d) {
          return height-yScale(d.measure);
      })
      .attr("fill", colorChosen)
      ;

    plot.selectAll("text.yAxis") // target the text element(s) which has a yAxis class defined
      .data(currentDatasetBarChart)
      .transition()
      .duration(750)
       .attr("text-anchor", "middle")
       .attr("x", function(d, i) {
          return (i * (width / currentDatasetBarChart.length)) + ((width / currentDatasetBarChart.length - barPadding) / 2);
       })
       .attr("y", function(d) {
          return yScale(d.measure) + 14;
       })
       .text(function(d) {
        return formatAsInteger(d3.round(d.measure));
       })
       .attr("class", "yAxis")
    ;


    svg.selectAll("text.title") // target the text element(s) which has a title class defined
      .attr("x", (width + margin.left + margin.right)/2)
      .attr("y", 15)
      .attr("class","title")
      .attr("text-anchor", "middle")
      .text(group + "")
    ;
}

function getBarChartData(group, data) {
    let datasetBarChart = [];
    for (let key in data) {
      let label = "";
      if (key.length > 15) {
        label = key.substring(0,12) + "...";
      } else {
        label = key;
      }
      datasetBarChart.push({ group: group, category: label, measure: data[key] })
    }
    datasetBarChart.sort(function(a,b) {
      return b.measure - a.measure;
    });
    console.log(datasetBarChart);
    return datasetBarChart;
}

$(function() {

  $.getJSON(BASE_API_URL + "/stats/summary", function(data) {
      let html = [];
      let inputs = [
        ["Current HBase Table", "table_name"],
        ["Total Tweets Collected", "total_tweets"],
        ["Total Search Phrases Used", "num_search_phrases"],
        ["Unique Categories", "num_categories"],
        ["Unique Product Tags","num_products"]
      ];
      for(let i = 0; i < inputs.length; i++) {
        html.push(
         wrap_li("<span class=\"tpr-item-title\">" + inputs[i][0] + ":</span> " + data[inputs[i][1]])
        );
      }
      $( "div#stats_summary" ).html( wrap_ul(html.join("")) );
  });

  $.getJSON(BASE_API_URL + "/stats/count/product", function(data) {
    let barChartData = getBarChartData("Product", data);
    console.log(barChartData);
    console.log(barChartData.slice(0,4));
    dsBarChart("#barchart-by-product", barChartData.slice(0,5), "By Title");
  });

  $.getJSON(BASE_API_URL + "/stats/count/category", function(data) {
    dsBarChart("#barchart-by-category", getBarChartData("Category", data), "By Category");
  });
});