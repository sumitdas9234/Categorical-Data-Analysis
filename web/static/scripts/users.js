const habit_desc = {
  PR: "Premium",
  VL: "Valuable",
  PO: "Potential",
  UN: "Uncommitted",
  LP: "Lapsing",
  GO: "GoneAway",
  "": "Missing"
};

function transpose(a) {
  return Object.keys(a[0]).map(function(c) {
    return a.map(function(r) {
      return r[c];
    });
  });
}

// const spend_desc = {
//   High: '<span class="lnr lnr-chevron-up"></span>',
//     Low: '<span class="lnr lnr- chevron - down"></span>',
//   Medium: ''
// };

$.getJSON(encodeURI($SCRIPT_ROOT + "/api/users"), function(data) {
  let keys = Object.keys(data[0]);

  $("#table_head").empty();
  $("#table_body").empty();

  for (key of keys) {
    $("#table_head").append(
      ` <th scope="col" class="border-0">
              ${key.toUpperCase()}
            </th>`
    );
  }

  for (field of data) {
    $("<tr>")
      .append(
        $("<td>").text(field.CustomerID),
        $("<td>").text("$ " + field.total_spend),
        $("<td>").text(field.weeks_shopped),
        $("<td>").text(habit_desc[field.shabit]),
        $("<td>").text(field.spend_desc),
        $("<td>").text(field.visit_desc)
      )
      .appendTo("#table_body");
  }
});

$.getJSON(encodeURI($SCRIPT_ROOT + "/api/users/distribution"), function(
  result
) {
  let ctx = $("#mw_stats");
  let ctx_2 = $("#mw_stats_2");
  let labels = Object.keys(result[0]);
  let data_ = [];
  console.log(result);

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
