import os
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions
from glob import glob
from datetime import datetime

# InfluxDB connection setup
influxdb_url = "http://localhost:8086"
token = "your_api_token_here"
org = "your_org"
bucket = "your_bucket"
csv_dir = "./your_aggregated_csv_folder"

# Create InfluxDB client and write API
client = InfluxDBClient(url=influxdb_url, token=token, org=org)
write_api = client.write_api(write_options=WriteOptions(
    batch_size=5000,
    flush_interval=2000,
    jitter_interval=500,
    retry_interval=5000,
    write_type="asynchronous"
))

# Loop through CSV files
for filepath in glob(os.path.join(csv_dir, "*.csv")):
    try:
        print(f"Processing file: {os.path.basename(filepath)}")

        # Load CSV with "-" as NaN
        df = pd.read_csv(filepath, na_values="-", low_memory=False)

        # Clean column names
        df.columns = [c.strip().strip('="') for c in df.columns]

        # Strip ="..." formatting from string values
        df = df.applymap(lambda x: str(x).strip().strip('="') if pd.notna(x) else x)

        # Convert numeric columns
        for col in df.columns:
            if col not in ["MP", "time_start", "window"]:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Parse timestamp from "time_start" column
        df["timestamp"] = pd.to_datetime(df["time_start"], format="%Y-%m-%d %H:%M:%S")
        timestamp_ns = df["timestamp"].astype("int64")  # nanosecond precision

        # Build Influx points
        points = []
        for idx, row in df.iterrows():
            measurement = row["MP"]
            window_tag = row.get("window", "unknown")

            point = Point(str(measurement)).tag("window", window_tag)

            # Add all numeric fields
            for field in [
                "Max", "Mean", "Min",
                "10", "25", "5", "50", "75",
                "90", "95", "99", "99.9",
                "message-count"
            ]:
                if field in row and pd.notna(row[field]):
                    safe_field = field.replace(".", "_")  # Replace dot for field compatibility
                    point = point.field(safe_field, float(row[field]))

            point = point.time(int(timestamp_ns.iloc[idx]), WritePrecision.NS)
            points.append(point)

        write_api.write(bucket=bucket, org=org, record=points)
        write_api.flush()
        print(f"  → Wrote {len(points)} points.")

    except Exception as e:
        print(f"  ✖ Error processing {filepath}: {e}")

client.close()
