$(function() {
  let ctx_pie_1 = $("#pie_1");
  $.getJSON($SCRIPT_ROOT + "/api/getKPI", function(data) {
    let kpi = JSON.parse(data);
    $("#t_count").html(kpi.transaction_count);
    $("#u_count").html(kpi.user_count);
    $("#c_count").html(kpi.country_count);
    $("#s_count").html(kpi.store_count);
    $("#p_count").html(kpi.product_count);
  });

  $("#blog-overview-date-range").datepicker({});
  [
    {
      backgroundColor: "rgba(0, 184, 216, 0.1)",
      borderColor: "rgb(0, 184, 216)",
      data: [0, 1, 2, 3, 4, 5, 6, 7, 8]
    },
    {
      backgroundColor: "rgba(23,198,113,0.1)",
      borderColor: "rgb(23,198,113)",
      data: [0, 1, 2, 3, 4, 5, 6, 7, 8]
    },
    {
      backgroundColor: "rgba(255,180,0,0.1)",
      borderColor: "rgb(255,180,0)",
      data: [0, 1, 2, 3, 4, 5, 6, 7, 8]
    },
    {
      backgroundColor: "rgba(255,65,105,0.1)",
      borderColor: "rgb(255,65,105)",
      data: [0, 1, 2, 3, 4, 5, 6, 7, 8]
    },
    {
      backgroundColor: "rgb(0,123,255,0.1)",
      borderColor: "rgb(0,123,255)",
      data: [0, 1, 2, 3, 4, 5, 6, 7, 8]
    }
  ].map(function(e, o) {
    var a = {
        maintainAspectRatio: !0,
        responsive: !0,
        legend: { display: !1 },
        tooltips: { enabled: !1, custom: !1 },
        elements: { point: { radius: 0 }, line: { tension: 0.3 } },
        scales: {
          xAxes: [{ gridLines: !1, scaleLabel: !1, ticks: { display: !1 } }],
          yAxes: [
            {
              gridLines: !1,
              scaleLabel: !1,
              ticks: {
                display: !1,
                suggestedMax: Math.max.apply(Math, e.data) + 1
              }
            }
          ]
        }
      },
      r = document.getElementsByClassName(
        "blog-overview-stats-small-" + (o + 1)
      );
    new Chart(r, {
      type: "line",
      data: {
        labels: [
          "Label 1",
          "Label 2",
          "Label 3",
          "Label 4",
          "Label 5",
          "Label 6",
          "Label 7"
        ],
        datasets: [
          {
            label: "Today",
            fill: "start",
            data: e.data,
            backgroundColor: e.backgroundColor,
            borderColor: e.borderColor,
            borderWidth: 1.5
          }
        ]
      },
      options: a
    });
  });

  $.getJSON($SCRIPT_ROOT + "/api/getLoyalty", function(data) {
    const habit_desc = {
      PR: "Premium",
      VL: "Valuable",
      PO: "Potential",
      UN: "Uncommitted",
      LP: "Lapsing",
      GO: "GoneAway",
      "": "Missing"
    };
    let pie_data = [];
    let labels = [];
    for (i of data) {
      pie_data.push(i.perc);
      labels.push(habit_desc[i.shabit]);
    }

    let pie_graph_1 = new Chart(ctx_pie_1, {
      type: "pie",
      data: {
        datasets: [
          {
            hoverBorderColor: "#ffffff",
            data: pie_data,
            backgroundColor: [
              "rgba(46, 204, 113,0.8)",
              "rgba(0,123,255,0.8)",
              "rgba(241, 196, 15,0.8)",
              "rgba(231, 76, 60,0.7)"
            ]
          }
        ],
        labels: labels
      },
      options: {
        legend: { position: "bottom", labels: { padding: 25, boxWidth: 20 } },
        cutoutPercentage: 0,
        tooltips: { custom: !1, mode: "index", position: "nearest" }
      }
    });
    pie_graph_1.render();
  });

  // let ctx = $("#mw_sales");
  // $.getJSON($SCRIPT_ROOT + "/api/getMonthwiseSales", function(data) {
  //   let mw_data = [];
  //   for (obj of data) {
  //     if (obj.year != 2010) {
  //       mw_data.push(obj.net_spend_amt);
  //     }
  //   }

  //   // Set the data points
  //   let datapoints = {
  //     labels: [
  //       "jan",
  //       "feb",
  //       "mar",
  //       "apr",
  //       "may",
  //       "jun",
  //       "jul",
  //       "aug",
  //       "sep",
  //       "oct",
  //       "nov",
  //       "dec"
  //     ],
  //     datasets: [
  //       {
  //         label: "Monthwise Sales",
  //         backgroundColor: "rgba(0, 184, 216, 0.1)",
  //         borderColor: "rgb(0, 184, 216)",
  //         data: mw_data
  //       }
  //     ]
  //   };
  //   // Set the options

  //   // Create the chart
  //   let lineGraph = new Chart(ctx, {
  //     type: "line",
  //     data: datapoints,
  //     options: {}
  //   });
  //   // Show the chart
  //   lineGraph.render();
  // });

  let ctx_pie_2 = $("#pie_2");
  let pie_graph_2 = new Chart(ctx_pie_2, {
    type: "bar",
    data: {
      datasets: [
        {
          label: "Visit Distribution (in %)",
          hoverBorderColor: "#ffffff",
          data: [68.3, 24.2, 7.5],
          backgroundColor: [
            "rgba(130, 88, 159, 1.0)",
            "rgba(214, 162, 232,1.0)",
            "rgba(179, 55, 113,1.0)"
          ]
        }
      ],
      labels: ["Weekly", "Bi-Weekly", "Often"]
    },
    options: {
      legend: { position: "bottom", labels: { padding: 25, boxWidth: 20 } },
      cutoutPercentage: 0,
      tooltips: { custom: !1, mode: "index", position: "nearest" }
    }
  });
  pie_graph_2.render();

  ctx_pie_1.click(function(e) {
    window.location.href = "/users";
  });

  let kpis = $(".clickable");
  kpis.click(function(e) {
    window.location.href = e.currentTarget.getAttribute("data-link");
  });
});
