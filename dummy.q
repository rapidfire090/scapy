// Define the root of the historical database
hdbPath: ":/hdb"

// Create the schema (partitioned by date)
.schema: `date xkey ([] date: date$(); timestamp: timestamp$(); source: symbol$(); speed: float$())

// Generate dummy data
n: 1000
dates: .z.D + til 3                        // Generate 3 days of data starting from today
sources: `sensor1`sensor2`sensor3

// Generate dummy table
genData: {
  date: x
  nDay: n div count dates
  timestamp: date + 0D00:00:00 + nDay?1D    // Random timestamps within the day
  source: nDay?sources
  speed: 50 + 10 * rand nDay                // Random speed values around 50
  flip `date`timestamp`source`speed! (enlist date, timestamp, source, speed)
}

// Create the HDB directories if they don't exist
system "mkdir -p ", hdbPath

// Save data partitioned by date
{ 
  d: x;
  t: genData d;
  path: hdbPath, "/", string d;
  system "mkdir -p ", path;
  `: path set t
  // Register the partition
  .Q.dpft[hdbPath; `:path; `.schema; `date]
 } each dates

// Done
"Dummy data saved to HDB"
