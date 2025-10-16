#!/usr/bin/env python3
"""
load_oltp_csvs.py

Bulk-load gzipped CSVs produced by the generator into MySQL using LOAD DATA LOCAL INFILE.

Usage:
  pip install pymysql
  python load_oltp_csvs.py --csv-dir /path/to/oltp_hackathon_full --host 127.0.0.1 --user myuser --database mydb --port 3306

Notes:
 - Script expects CSVs named like customers.csv.gz, products.csv.gz,
   chunk_01_orders.csv.gz, chunk_01_order_items.csv.gz, chunk_01_payments.csv.gz,
   chunk_01_shipments.csv.gz, chunk_01_returns.csv.gz, product_bundles.csv.gz, bundle_items.csv.gz, product_price_history.csv.gz
 - If files are gzipped (they are), the script gunzips them to /tmp then uses LOAD DATA LOCAL INFILE.
 - For Cloud SQL, run this script on a VM with Cloud SQL Proxy or network access to the DB.
"""

import os
import glob
import argparse
import tempfile
import subprocess
import pymysql
import getpass
import sys
import time

TABLE_COLUMN_MAP = {
    "customers": "customer_id,first_name,last_name,email,phone,city,state,signup_date,is_active,external_id,guest_flag",
    "products": "product_id,product_name,product_category_raw,list_price,is_active,created_at,cost_price",
    "product_bundles": "bundle_id,bundle_name",
    "bundle_items": "bundle_item_id,bundle_id,product_id,quantity,bundle_price",
    "product_price_history": "id,product_id,start_date,end_date,list_price,promotion_id",
    "orders": "order_id,customer_id,order_date,status,order_placed_at,channel,user_agent,device_type,utm_source,order_value,num_items",
    "order_items": "order_item_id,order_id,product_id,quantity,unit_price,line_total",
    "payments": "payment_id,order_id,payment_method,amount,payment_date,is_refunded",
    "shipments": "shipment_id,order_id,shipped_date,delivered_date,delivery_city,shipment_status,carrier",
    "returns": "return_id,order_id,order_item_id,returned_date,reason_code,refund_amount"
}

# File name patterns to table mapping (gz files)
FILENAME_TABLE_MAP = {
    "customers.csv.gz": "customers",
    "products.csv.gz": "products",
    "product_bundles.csv.gz": "product_bundles",
    "bundle_items.csv.gz": "bundle_items",
    "product_price_history.csv.gz": "product_price_history",
    # chunked ones: will match via pattern
    "_orders.csv.gz": "orders",
    "_order_items.csv.gz": "order_items",
    "_payments.csv.gz": "payments",
    "_shipments.csv.gz": "shipments",
    "_returns.csv.gz": "returns"
}

def find_files(csv_dir):
    """Return list of (filepath, table) discovered in csv_dir."""
    found = []
    # exact matches
    for fname, table in FILENAME_TABLE_MAP.items():
        if fname.startswith("_"):  # skip chunk suffix patterns
            continue
        path = os.path.join(csv_dir, fname)
        if os.path.exists(path):
            found.append((path, table))
    # chunk patterns (any file ending with pattern)
    for pattern_suffix, table in [(k, v) for k,v in FILENAME_TABLE_MAP.items() if k.startswith("_")]:
        glob_pattern = os.path.join(csv_dir, f"*{pattern_suffix}")
        for path in sorted(glob.glob(glob_pattern)):
            found.append((path, table))
    return found

def gunzip_to_temp(gz_path):
    """Gunzip gz_path to a temp file and return its path."""
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="oltp_load_", suffix=".csv")
    os.close(tmp_fd)
    try:
        # use system gunzip for streaming speed
        with open(tmp_path, "wb") as out_f:
            subprocess.run(["gunzip", "-c", gz_path], check=True, stdout=out_f)
    except subprocess.CalledProcessError as e:
        # cleanup
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    return tmp_path

def load_file_into_table(conn, tmp_csv_path, table, columns):
    """Execute LOAD DATA LOCAL INFILE for the given tmp csv into table."""
    cursor = conn.cursor()
    # Build LOAD DATA statement. Use local infile.
    sql = f"""
    LOAD DATA LOCAL INFILE %s
    INTO TABLE `{table}`
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    ({columns});
    """
    try:
        start = time.time()
        cursor.execute(sql, (tmp_csv_path,))
        conn.commit()
        elapsed = time.time() - start
        print(f"[OK] Loaded {os.path.basename(tmp_csv_path)} -> {table} ({cursor.rowcount} rows affected) in {elapsed:.2f}s")
    except Exception as e:
        conn.rollback()
        raise

def main():
    parser = argparse.ArgumentParser(description="Bulk load OLTP CSV.gz into MySQL")
    parser.add_argument("--csv-dir", required=True, help="Directory containing CSV.gz files")
    parser.add_argument("--host", required=True, help="MySQL host")
    parser.add_argument("--user", required=True, help="MySQL user")
    parser.add_argument("--database", required=True, help="MySQL database")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--ssl", action="store_true", help="Use SSL (pass SSL params via env or client config)")
    parser.add_argument("--skip-fk-disable", action="store_true", help="Do not disable foreign key checks during load")
    args = parser.parse_args()

    password = getpass.getpass(prompt="MySQL password: ")

    csv_dir = args.csv_dir
    if not os.path.isdir(csv_dir):
        print("ERROR: csv-dir not found:", csv_dir)
        sys.exit(2)

    files = find_files(csv_dir)
    if not files:
        print("No CSV.gz files found in", csv_dir)
        sys.exit(1)

    print(f"Found {len(files)} files to load.")
    for p,t in files:
        print(" -", os.path.basename(p), "->", t)

    # connect to MySQL with local_infile enabled
    try:
        conn = pymysql.connect(host=args.host, user=args.user, password=password, database=args.database, port=args.port, local_infile=1, autocommit=False)
    except Exception as e:
        print("ERROR connecting to MySQL:", e)
        sys.exit(3)

    try:
        cur = conn.cursor()
        if not args.skip_fk_disable:
            print("Disabling foreign_key_checks to speed up bulk load...")
            cur.execute("SET FOREIGN_KEY_CHECKS=0;")
            conn.commit()

        # Load stable single files first (customers, products, bundles, bundle_items, price history)
        ordered_tables = ["customers","products","product_bundles","bundle_items","product_price_history"]
        # Then load chunked data (orders, order_items, payments, shipments, returns)
        chunk_tables_order = ["orders","order_items","payments","shipments","returns"]

        # build map of discovered files by table
        files_by_table = {}
        for path, table in files:
            files_by_table.setdefault(table, []).append(path)

        # 1) load ordered_tables if present
        for table in ordered_tables:
            paths = files_by_table.get(table, [])
            for path in paths:
                print(f"\nProcessing file: {path}")
                tmp = None
                try:
                    tmp = gunzip_to_temp(path)
                    cols = TABLE_COLUMN_MAP.get(table)
                    if cols is None:
                        raise RuntimeError(f"No column mapping for table {table}")
                    load_file_into_table(conn, tmp, table, cols)
                except Exception as e:
                    print(f"[ERROR] Failed to load {path} into {table}: {e}")
                    raise
                finally:
                    if tmp and os.path.exists(tmp):
                        os.remove(tmp)

        # 2) load chunked tables in logical order
        # We should load orders first (to satisfy fk), then order_items, payments, shipments, returns
        for table in chunk_tables_order:
            paths = files_by_table.get(table, [])
            # sort paths for deterministic order (chunk_01, chunk_02, ...)
            paths = sorted(paths)
            for path in paths:
                print(f"\nProcessing file: {path}")
                tmp = None
                try:
                    tmp = gunzip_to_temp(path)
                    cols = TABLE_COLUMN_MAP.get(table)
                    if cols is None:
                        raise RuntimeError(f"No column mapping for table {table}")
                    load_file_into_table(conn, tmp, table, cols)
                except Exception as e:
                    print(f"[ERROR] Failed to load {path} into {table}: {e}")
                    raise
                finally:
                    if tmp and os.path.exists(tmp):
                        os.remove(tmp)

        # finished loading
        if not args.skip_fk_disable:
            print("Re-enabling foreign_key_checks...")
            cur.execute("SET FOREIGN_KEY_CHECKS=1;")
            conn.commit()

        print("\nAll files loaded successfully.")
    except Exception as e:
        print("Fatal error during load:", e)
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=1;")
            conn.commit()
        except:
            pass
        sys.exit(4)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
