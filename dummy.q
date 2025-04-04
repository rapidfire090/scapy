// Set the HDB root path (relative to current directory)
hdbPath: "./hdb"

// Define the partitioned table schema using empty typed lists
.schema: `date xkey ([] 
  date: 0D#0d;          // empty list of dates
  timestamp: 0#0Np;      // empty list of timestamps
  source: 0#`;           // empty list of symbols
  speed: 0#0n            // empty list of floats
)

// Parameters
n: 1000
dates: .z.D + til 3       // 3 days starting from today
sources: `sensor1`sensor2`sensor3

// Function to generate dummy data for one day
genData: {
  d: x;
  rows: n div count dates;
  // Generate a list of timestamps within the day
  timestamp: d + 0D00:00:00 + rows?1D;
  // Randomly select a source value for each row
  source: rows?sources;
  // Generate random speed values around 50
  speed: 50 + 10 * rand rows;
  // Create a table with all columns; date is the same for every row
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
