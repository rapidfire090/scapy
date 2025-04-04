// Define HDB root path (relative to current directory)
hdbPath: "./hdb"

// Create schema (partitioned by date)
.schema: `date xkey ([] date: date$(); timestamp: timestamp$(); source: symbol$(); speed: float$())

// Parameters
n: 1000
dates: .z.D + til 3                        // 3 days starting from today
sources: `sensor1`sensor2`sensor3

// Function to generate dummy data for one day
genData: {
  date: x;
  nDay: n div count dates;
  timestamp: date + 0D00:00:00 + nDay?1D;
  source: nDay?sources;
  speed: 50 + 10 * rand nDay;
  flip `date`timestamp`source`speed! (enlist date, timestamp, source, speed)
}

// Create base HDB folder (using q command)
if[not hdbPath in system "ls"; system "mkdir ", hdbPath];

// Generate and save data per day
{
  d: x;
  t: genData d;
  path: hdbPath , "/" , string d;
  if[not path in system "ls"; system "mkdir ", path];
  `:path/set t;
  .Q.dpft[hdbPath; `:path; `.schema; `date]
 } each dates;

"✅ Dummy data written to HDB successfully"
