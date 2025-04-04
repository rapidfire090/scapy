// Root HDB path
hdbPath: "./hdb"

// Define table schema using correct types in spec.tables
spec.tables: `speedtbl!(<[
  date: `date;
  timestamp: `timestamp;
  source: `symbol;
  speed: `real       / <- THIS is the correct type name
])

// Create an empty table schema (keyed on `date`)
.schema: `date xkey ([] 
  date: 0D#0d;
  timestamp: 0#0Np;
  source: `symbol$();
  speed: 0#0n
)

// Parameters
n: 1000
dates: .z.D + til 3
sources: `sensor1`sensor2`sensor3

// Function to generate dummy data
genData: {
  d: x;
  rows: n div count dates;
  timestamp: d + 0D00:00:00 + rows?1D;
  source: rows?sources;
  speed: 50 + 10 * rand rows;
  flip `date`timestamp`source`speed! (enlist d, timestamp, source, speed)
}

// Ensure HDB base path exists
if[not hdbPath in system "ls"; system "mkdir ", hdbPath];

// Generate and save partitioned data using spec.tables
{
  d: x;
  t: genData d;
  path: hdbPath , "/" , string d;
  if[not path in system "ls"; system "mkdir ", path];
  `:path/set t;
  .Q.dpft[hdbPath; `:path; spec.tables[`speedtbl]; `date]
} each dates;

"✅ Dummy data saved to HDB using spec.tables"
