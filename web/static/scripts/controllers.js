$(function() {
  $(".li-file").bind("click", function(e) {
    e.preventDefault();
    let filepath = e.currentTarget.getAttribute("data-path");
    $.getJSON(
      $SCRIPT_ROOT + "/load_file",
      {
        filepath: filepath
      },
      function(data) {
        let schema = JSON.parse(data);
        console.log(schema);

        let keys = Object.keys(schema.fields[0]);
        let cols = schema.fields.length;
        let filename = filepath.split("/").pop();

        $("#table_head").empty();
        $("#table_body").empty();
        $("#file_header").html(
          `Found ${cols} columns in <strong>${filename}</strong>`
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
              $("<td>").text(field.nullable),
              $("<td>").text(field.type)
            )
            .appendTo("#table_body");
        }
      }
    );
  });
});
// e.currentTarget.getAttribute("data-path")
