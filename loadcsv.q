// Define file paths
csvPath: ":/path/to/your/data.csv"     // Replace with actual CSV file path
hdbPath: ":/hdb"                       // Replace with your HDB location

// Define schema
schema: `mytable xkey `timestamp`source!(`timestamp`sym; `symbol`)

// Define partitioned column
partCol: `timestamp

// Load CSV data
rawData: ("PSSSS"; ",") 0: csvPath

// Assign column names
cols: `timestamp`source`speed`state`city
data: flip cols!rawData

// Type casting
data[`timestamp]: .z.P each data[`timestamp]     // parse timestamps
data[`source]: `symbol$ data[`source]
data[`speed]: "F"$ data[`speed]
data[`state]: `symbol$ data[`state]
data[`city]: `symbol$ data[`city]

// Create and save HDB structure
system "mkdir ", hdbPath
`:hdbPath set schema

// Insert data into table
\l hdbPath
upd: insert[`mytable]
upd[data]