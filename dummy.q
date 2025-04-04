// Set the HDB root path (relative to current directory)
hdbPath: "./hdb"

// Define the partitioned table schema with int speed column
.schema: `date xkey ([
  date: 0D#0d
  ];
  timestamp: 0#0Np;
  source: `symbol$();
  speed: int$()
)

// Parameters
n: 1000
dates: .z.D + til 3
sources: `sensor1`sensor2`sensor3

// Corrected dummy data generator
genData: {
  d: x;
  rows: n div count dates;
  timestamp: d + 0D00:00:00 + rows?1D;
  source: rows?sources;
  speed: 50 + 10 * rand rows;
  speedInt: floor speed;
  date: rows#d;
  flip `date`timestamp`source`speed! (date; timestamp; source; speedInt)
}

// Create the base HDB directory if needed
if[not hdbPath in system "ls"; system "mkdir ", hdbPath];

// Generate and save partitioned data
{
  d: x;
  t: genData d;
  path: hdbPath , "/" , string d;
  if[not path in system "ls"; system "mkdir ", path];
  `:path/set t;
  .Q.dpft[hdbPath; `:path; `.schema; `date]
} each dates;

"✅ Dummy data written to HDB with speed as int"
