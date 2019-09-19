const habit_desc = {
  PR: "Platinum",
  VL: "Diamond",
  PO: "Gold",
  UN: "Silver"
};
// const spend_desc = {
//   High: '<span class="lnr lnr-chevron-up"></span>',
//     Low: '<span class="lnr lnr- chevron - down"></span>',
//   Medium: ''
// };

$.getJSON(encodeURI($SCRIPT_ROOT + "/api/users/distribution"), function(
  result
) {
  let ctx = $("#mw_stats");
  let ctx_2 = $("#mw_stats_2");
  let labels = Object.keys(result[0]);
  let data_ = [];

  for (let i = 0; i < 5; i++) {
    d = [];
    for (let j = 0; j < 4; j++) {
      d.push(result[j][labels[i]]);
    }
    data_.push(d);
  }
  let barchartdata = {
    labels: [
      habit_desc[result[0].shabit],
      habit_desc[result[1].shabit],
      habit_desc[result[2].shabit],
      habit_desc[result[3].shabit]
    ],
    datasets: [
      {
        label: "weeks_shopped",
        backgroundColor: "pink",
        borderColor: "red",
        borderWidth: 1,
        data: data_[1]
      },
      {
        label: "max_consec_weeks",
        backgroundColor: "lightblue",
        borderColor: "blue",
        borderWidth: 1,
        data: data_[2]
      },
      {
        label: "average_visits",
        backgroundColor: "lightgreen",
        borderColor: "green",
        borderWidth: 1,
        data: data_[3]
      },
      {
        label: "distinct_periods",
        backgroundColor: "yellow",
        borderColor: "orange",
        borderWidth: 1,
        data: data_[4]
      }
    ]
  };

  let barChart = new Chart(ctx, {
    type: "bar",
    data: barchartdata,
    options: {
      legend: {
        position: "top"
      },
      scales: {
        yAxes: [
          {
            ticks: {
              beginAtZero: true
            }
          }
        ]
      }
    }
  });

  labels = Object.keys(result[4]);
  data_ = [];
  for (let i = 0; i < 4; i++) {
    d = [];
    for (let j = 4; j < 8; j++) {
      d.push(result[j][labels[i]]);
    }
    data_.push(d);
  }

  let barchartdata_2 = {
    labels: [
      habit_desc[result[4].shabit],
      habit_desc[result[5].shabit],
      habit_desc[result[6].shabit],
      habit_desc[result[7].shabit]
    ],
    datasets: [
      {
        label: "avg_spend",
        backgroundColor: "pink",
        borderColor: "red",
        borderWidth: 1,
        data: data_[1]
      },
      {
        label: "med_spend",
        backgroundColor: "lightblue",
        borderColor: "blue",
        borderWidth: 1,
        data: data_[2]
      },
      {
        label: "total_spend",
        backgroundColor: "lightgreen",
        borderColor: "green",
        borderWidth: 1,
        data: data_[3]
      }
    ]
  };

  let barChart_2 = new Chart(ctx_2, {
    type: "bar",
    data: barchartdata_2,
    options: {
      legend: {
        position: "top"
      },
      scales: {
        yAxes: [
          {
            ticks: {
              beginAtZero: false
            }
          }
        ]
      }
    }
  });

  barChart.render();
  barChart_2.render();
});

$.getJSON(encodeURI($SCRIPT_ROOT + "/api/topCustomers"), function(data) {
  let po = $("#po");
  let pr = $("#pr");
  let vl = $("#vl");
  let un = $("#un");

  for (let row of data) {
    if (row.shabit == "PO") {
      po.append(`<li class="list-group-item d-flex px-3">
              <span class="text-semibold text-fiord-blue">${
                row.CustomerID
              }</span>
              <span class="ml-auto text-right text-semibold text-reagent-gray"
                >${"$ " + row.avg_spend}</span
              >
            </li>`);
    } else if (row.shabit == "PR") {
      pr.append(`<li class="list-group-item d-flex px-3">
              <span class="text-semibold text-fiord-blue">${
                row.CustomerID
              }</span>
              <span class="ml-auto text-right text-semibold text-reagent-gray"
                >${"$ " + row.avg_spend}</span
              >
            </li>`);
    } else if (row.shabit == "VL") {
      vl.append(`<li class="list-group-item d-flex px-3">
              <span class="text-semibold text-fiord-blue">${
                row.CustomerID
              }</span>
              <span class="ml-auto text-right text-semibold text-reagent-gray"
                >${"$ " + row.avg_spend}</span
              >
            </li>`);
    } else {
      un.append(`<li class="list-group-item d-flex px-3">
              <span class="text-semibold text-fiord-blue">${
                row.CustomerID
              }</span>
              <span class="ml-auto text-right text-semibold text-reagent-gray"
                >${"$ " + row.avg_spend}</span
              >
            </li>`);
    }
  }
});
