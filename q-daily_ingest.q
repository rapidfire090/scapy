/ === CONFIGURATION DEFAULTS ===
defaultDataDir: "/data"
defaultFilePattern: "*.csv"


/ === ARGUMENT PARSER ===
parseArgs: {
  params: parse key`value each (";" vs raze string x);
  get: { $[key in key params; value params where key=params; y] }
  dict[`date]!enlist get[`date; string .z.d],
  dict[`dir]!enlist get[`dir; defaultDataDir],
  dict[`pattern]!enlist get[`pattern; defaultFilePattern]
}


/ === PERCENTILE CONFIGURATION ===
hdbPath: `:/hdb
percentileList: (1 25 50 75 99 99.9)
percentileCols: ("p", string each percentileList)
percentileColSyms: `p1`p25`p50`p75`p99`p99_9


/ === LOAD CSV TO HDB WITH SOURCE TAGGING ===
loadTradeCSVToHDB: {[csvPath; sourceSym]
  raw: ("TSCJFS"; enlist ",") 0: csvPath;
  raw[`timestamp]: .z.N each raw[`timestamp];
  raw[`msgType]: `char$ raw[`msgType];
  raw[`source]: sourceSym;
  raw[`date]: date each raw[`timestamp];

  `tradeTable upsert raw;
  .Q.dpft[hdbPath; `.; enlist[`tradeTable]; `timestamp; ()];

  distinct select distinct date, source from raw
}


/ === CALCULATE PERCENTILES FOR NEWLY LOADED DATA ===
generateAndSaveDailyPercentiles: {[injectedMeta]
  do[count injectedMeta; {
    d: injectedMeta[i;`date];
    s: injectedMeta[i;`source];

    t: select from hdbPath, `tradeTable where date=d, source=s;
    grouped: select speed by timebin: 5 xbar timestamp from t;

    result: select timebin, 
            (enlist each percentileCols)!((percentileList pct speed) each speed) 
            from grouped;

    structured: flip update timebin: timebin, date: d, source: s from result;
    rename: update (enlist[`p99.9])#`p99_9 from structured;

    `speedPercentiles5min upsert rename;
    .Q.dpft[hdbPath; `.; enlist[`speedPercentiles5min]; `timebin; ()];
  } each til count injectedMeta];
}


/ === MAIN RUN FUNCTION (ENTRY POINT) ===
runDailyIngest: {[args]
  config: parseArgs args;
  dt: .z.d $ config[`date];
  dir: config[`dir];
  pattern: config[`pattern];

  files: system "ls ", dir, "/", pattern;
  todayFiles: files where string dt in/: files;

  allMeta: ();
  do[count todayFiles; {
    path: dir, "/", todayFiles[i];
    parts: "_" vs last "/" vs path;
    source: first parts;

    meta: loadTradeCSVToHDB[path; `symbol$source];
    allMeta,: meta;
  }];

  generateAndSaveDailyPercentiles allMeta;
}
