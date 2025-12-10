import wrds
import pandas as pd
from sqlalchemy import text
import os
import pyarrow.parquet as pq
import pyarrow as pa

db = wrds.Connection(wrds_username='rylanmiller25')

db.list_tables(library="revelio")

sql_query = text("""
SELECT *
FROM revelio.company_mapping 
""")


df = pd.read_sql_query(
    sql_query,
    db.connection
)

# Target directory
out_dir = "/scratch/zt1/project/estarr-prj/shared/Revelio/Raw"

# Full output path
out_path = os.path.join(out_dir, "company_mapping.parquet")

# Convert pandas dataframe to Arrow table
table = pa.Table.from_pandas(df, preserve_index=False)

# Save as Parquet
pq.write_table(table, out_path)

print(f"Saved Parquet file to: {out_path}")


