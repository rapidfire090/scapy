import "experimental"

percentiles = ["p50", "p90", "p95", "99_9"]

data = from(bucket: "your_bucket")
  |> range(start: -1h)  // narrow to recent data
  |> filter(fn: (r) => 
    r._measurement == "aggregated_stats" and 
    contains(value: r._field, set: percentiles) and
    r.MP == "SensorA"  // filter by tag if needed
  )
  |> last()  // get the most recent value per field
  |> keep(columns: ["_field", "_value"])

experimental.toRows()
