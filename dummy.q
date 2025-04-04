// Define HDB root path (relative to where script is run)
hdbPath: "./hdb"

// Define the table schema (partitioned by date)
.schema: `date xkey ([
  date: enlist 0D
  ];
  timestamp: enlist 0Np;
  source: enlist `symbol$();
  speed: enlist 0n
)

// Parameters
n: 1000
dates: .z.D + til 3                         // 3 days starting from today
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

// Create base HDB folder if it doesn't exist
if[not hdbPath in system "ls"; system "mkdir ", hdbPath];

// Generate and save data for each day
{
  d: x;
  t: genData d;
  path: hdbPath , "/" , string d;
  if[not path in system "ls"; system "mkdir ", path];
  `:path/set t;
  .Q.dpft[hdbPath; `:path; `.schema; `date]
 } each dates;

"✅ Dummy data written to HDB successfully"
