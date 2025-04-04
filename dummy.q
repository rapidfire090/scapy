// Set the HDB root path (relative to current directory)
hdbPath: "./hdb"

// Define empty lists for the schema with explicit types
emptyDates: 0D#0d;         / empty list of dates
emptyTimestamps: 0#0Np;     / empty list of timestamps
emptySymbols: `symbol$();   / empty list of symbols, by casting an empty list to symbol type
emptyFloats: 0#0n;          / empty list of floats

// Define the partitioned table schema using the above empty lists
.schema: `date xkey ([
  date: emptyDates
  ];
  timestamp: emptyTimestamps;
  source: emptySymbols;
  speed: emptyFloats
)

// Parameters
n: 1000
dates: .z.D + til 3         / 3 days starting from today
sources: `sensor1`sensor2`sensor3

// Function to generate dummy data for one day
genData: {
  d: x;
  rows: n div count dates;
  timestamp: d + 0D00:00:00 + rows?1D;  / Random timestamps within the day
  source: rows?sources;                / Randomly pick a sensor source for each row
  speed: 50 + 10 * rand rows;           / Random speed values around 50
  flip `date`timestamp`source`speed! (enlist d, timestamp, source, speed)
}

// Create the base HDB directory if it doesn't exist
if[not hdbPath in system "ls"; system "mkdir ", hdbPath];

// Generate and save partitioned data for each date
{
  d: x;
  t: genData d;
  path: hdbPath , "/" , string d;
  if[not path in system "ls"; system "mkdir ", path];
  `:path/set t;
  .Q.dpft[hdbPath; `:path; `.schema; `date]
} each dates;

"✅ Dummy data written to HDB successfully"
