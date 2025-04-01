import polars as pl
import pandas as pd
import yaml
import requests
import os
import hashlib
import smtplib
from io import StringIO
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# --- Config ---
CACHE_DIR = "cached_sources"
os.makedirs(CACHE_DIR, exist_ok=True)
source_cache = {}

# --- Helpers ---

def get_cache_filename(source_name: str, date_range: dict) -> str:
    start = date_range.get("start_date", "")
    end = date_range.get("end_date", "")
    range_id = f"{start}_{end}"
    hash_id = hashlib.md5(range_id.encode()).hexdigest()[:8]
    return os.path.join(CACHE_DIR, f"{source_name}_{hash_id}.parquet")


def fetch_csv_from_source(source_name: str, download_sources: dict, auth: dict, date_range: dict, use_cache_dir: bool) -> pl.DataFrame:
    if source_name in source_cache:
        return source_cache[source_name]

    parquet_path = get_cache_filename(source_name, date_range)

    if use_cache_dir and os.path.exists(parquet_path):
        df = pl.read_parquet(parquet_path)
        source_cache[source_name] = df
        return df

    url = download_sources.get(source_name)
    if not url:
        raise ValueError(f"No download URL found for source: {source_name}")

    user = auth.get("user")
    password = auth.get("password")
    auth_tuple = (user, password) if user and password else None

    params = {
        "start_date": date_range.get("start_date"),
        "end_date": date_range.get("end_date")
    }
    params = {k: v for k, v in params.items() if v is not None}

    response = requests.get(url, auth=auth_tuple, params=params)
    response.raise_for_status()

    df = pl.read_csv(StringIO(response.text))

    if use_cache_dir:
        df.write_parquet(parquet_path)

    source_cache[source_name] = df
    return df


def send_email_report(subject, sender, to_list, cc_list, df_out, smtp_config, filter_df):
    html = f"""
    <html>
        <body>
            <h2>{subject}</h2>
            {df_out.to_html(border=0, classes='dataframe', index=True)}
            <br><br>
            <h3>Filters Used</h3>
            {filter_df.to_html(index=False, border=0, classes='filters')}
        </body>
    </html>
    """

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(to_list)
    msg['Cc'] = ", ".join(cc_list)
    msg['Subject'] = subject
    msg.attach(MIMEText(html, 'html'))

    recipients = to_list + cc_list

    with smtplib.SMTP(smtp_config['host'], smtp_config.get('port', 587)) as server:
        if smtp_config.get('use_tls', True):
            server.starttls()
        server.login(smtp_config['username'], smtp_config['password'])
        server.sendmail(sender, recipients, msg.as_string())


# --- Load YAML Config ---
with open("config.yaml", "r") as f:
    full_config = yaml.safe_load(f)

# Global settings
default_percentiles = full_config.pop("default_percentiles", [0.5, 0.75, 0.9, 0.99])
sum_percentiles = full_config.pop("sum_percentiles", False)
exclude_from_sum = set(full_config.pop("exclude_from_sum", []))
row_order = full_config.pop("row_order", None)
use_cache_dir = full_config.pop("use_cache_dir", True)
auth = full_config.pop("auth", {})
date_range = full_config.pop("date_range", {})
download_sources = full_config.pop("download_sources", {})

# Email settings
email_config = full_config.pop("email", {})
email_subject = email_config.get("subject", "Polars Summary Report")
email_from = email_config.get("from")
email_to = email_config.get("to", [])
email_cc = email_config.get("cc", [])
smtp_config = email_config.get("smtp", {})

results = []
included_for_sum = []
filter_rows = []

# --- Process Sections ---
for section_name, section_data in full_config.items():
    source_name = section_data["source"]
    rename_map = section_data.get("rename", {})
    filters = section_data.get("filters", {})

    df = fetch_csv_from_source(source_name, download_sources, auth, date_range, use_cache_dir)

    if rename_map:
        df = df.rename(rename_map)

    conditions = []
    for column, filter_def in filters.items():
        if isinstance(filter_def, dict):
            filter_type = filter_def.get("type", "exact")
            filter_value = filter_def.get("value")
        else:
            filter_type = "exact"
            filter_value = filter_def

        # Record filter info for email
        filter_rows.append({
            "Source": section_name,
            "Column": column,
            "Type": filter_type,
            "Value": str(filter_value)
        })

        # Build filter condition
        if filter_type == "exact":
            if not isinstance(filter_value, list):
                filter_value = [filter_value]
            conditions.append(pl.col(column).is_in(filter_value))
        elif filter_type == "regex":
            conditions.append(pl.col(column).str.contains(filter_value))
        else:
            raise ValueError(f"Unknown filter type: {filter_type}")

    if conditions:
        combined_filter = conditions[0]
        for cond in conditions[1:]:
            combined_filter &= cond
        df = df.filter(combined_filter)

    stat_exprs = [
        pl.col("speed").quantile(p, "nearest").alias(f"p{int(p*100)}")
        for p in default_percentiles
    ] + [
        pl.col("speed").min().alias("min"),
        pl.col("speed").max().alias("max")
    ]

    stats = df.select(stat_exprs).with_columns(
        pl.lit(section_name).alias("source")
    )

    results.append(stats)

    if section_name not in exclude_from_sum:
        included_for_sum.append(stats)

# --- Combine and Summarize ---
final_df = pl.concat(results)

if sum_percentiles and included_for_sum:
    summed = pl.concat(included_for_sum).drop("source").sum().with_columns(
        pl.lit("TOTAL_SUM").alias("source")
    )
    final_df = pl.concat([final_df, summed])

df_out = final_df.to_pandas().set_index("source")
filter_df = pd.DataFrame(filter_rows)

if row_order:
    df_out = df_out.reindex(row_order)

# --- Send Email or Print ---
if email_to:
    send_email_report(
        subject=email_subject,
        sender=email_from,
        to_list=email_to,
        cc_list=email_cc,
        df_out=df_out,
        smtp_config=smtp_config,
        filter_df=filter_df
    )
else:
    print("\n--- Percentiles Summary Table ---")
    print(df_out)
    print("\n--- Filters Used ---")
    print(filter_df)
