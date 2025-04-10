from(bucket: "your_bucket")
  |> range(start: -30d)  // adjust as needed (e.g. -7d, -90d, or a fixed start time)
  |> filter(fn: (r) => 
    r._measurement == "aggregated_stats" and 
    (r._field == "p50" or r._field == "p90" or r._field == "p95")
  )
  |> filter(fn: (r) => r.MP == "SensorA")  // optional: filter by MP tag
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)  // smooth over time (optional)
  |> yield(name: "percentiles_over_time")
