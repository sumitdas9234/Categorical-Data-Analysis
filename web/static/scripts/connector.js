$(document).ready(function() {
  $.ajax({
    crossOrigin: true,
    url: "http://localhost:4041/api/v1/applications",
    type: "OPTIONS",
    contentType: "application/json",
    dataType: "json",
    success: function(result) {
      console.log(result);
    }
  });
});

$(function() {
  $(".li-file").bind("click", function(e) {
    e.preventDefault();

    let filepath = e.currentTarget.getAttribute("data-path");

    $.getJSON(encodeURI($SCRIPT_ROOT + "/api/load_csv/" + filepath), function(
      data
    ) {
      let schema = JSON.parse(data);
      console.log(schema);

      let keys = Object.keys(schema.fields[0]);
      let cols = schema.fields.length;
      let filename = filepath.split("\\").pop();

      $("#table_head").empty();
      $("#table_body").empty();
      $("#file_header").html(
        `Found ${cols} columns in <strong class="text-muted">${filename}</strong>`
      );

      for (key of keys.splice(1, keys.length)) {
        $("#table_head").append(
          ` <th scope="col" class="border-0">
              ${key.toUpperCase()}
            </th>`
        );
      }

      for (field of schema.fields) {
        $("<tr>")
          .append(
            $("<td>").text(field.name),
            $("<td>").html(
              field.nullable
                ? "<i class='lnr lnr-checkmark-circle' style='font-size:1.05rem; color: #2ecc71;'></i>"
                : "<i class='lnr lnr-cross-circle' style='font-size:1.05rem; color: #ff7675;'></i>"
            ),
            $("<td>").text(field.type)
          )
          .appendTo("#table_body");
      }
    });
  });
});
