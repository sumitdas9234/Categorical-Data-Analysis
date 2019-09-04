$(function() {
  let ctx = $("#mw_sales");
  $.getJSON($SCRIPT_ROOT + "/get_data_points", function(data) {
    let mw_data = [];
    for (obj of data) {
      if (obj.year != 2010) {
        mw_data.push(obj.net_spend_amt);
      }
    }

    // Set the data points
    let datapoints = {
      labels: [
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec"
      ],
      datasets: [
        {
          label: "Monthwise Sales",
          backgroundColor: "rgba(0, 184, 216, 0.1)",
          borderColor: "rgb(0, 184, 216)",
          data: mw_data
        }
      ]
    };
    // Set the options

    // Create the chart
    let lineGraph = new Chart(ctx, {
      type: "line",
      data: datapoints,
      options: {}
    });
    // Show the chart
    lineGraph.render();
  });
});
