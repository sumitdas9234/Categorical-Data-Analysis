$(function() {
  let ctx = $("#mw_sales");
  $.getJSON($SCRIPT_ROOT + "/api/getMonthwiseSales", function(data) {
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

  $.getJSON($SCRIPT_ROOT + "/api/getKPI", function(data) {
    let kpi = JSON.parse(data);
    $("#t_count").html(kpi.transaction_count);
    $("#u_count").html(kpi.user_count);
    $("#c_count").html(kpi.country_count);
    $("#s_count").html(kpi.store_count);
    $("#p_count").html(kpi.product_count);

    console.log(
      kpi.transaction_count,
      kpi.user_count,
      kpi.country_count,
      kpi.store_count,
      kpi.product_count
    );
  });
});
