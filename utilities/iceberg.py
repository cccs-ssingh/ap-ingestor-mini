import logging

def format_size(bytes_size):
    """
    Convert bytes to a human-readable format (KB, MB, GB, etc.).
    """
    if bytes_size is None or bytes_size == 0:
        return "0 B"

    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024

def seconds_to_hh_mm_ss(seconds):
    # Calculate the time components
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    # Format the time as HH:MM:SS
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

# Function to inspect the snapshots table
def inspect_snapshots_table(spark, iceberg_table):
    # Load the snapshots table
    snapshots_df = spark.read.format("iceberg").load(f"{iceberg_table}.snapshots")

    # Show all columns and data for debugging
    snapshots_df.show(truncate=False)

    # Print the available columns in the snapshots table
    print("Available columns in snapshots table:", snapshots_df.columns)

def get_latest_snapshot(spark, iceberg_table):
    """
    Retrieves the latest snapshot of an Iceberg table.

    Parameters:
        spark (SparkSession): The active Spark session.
        iceberg_table (str): The full table identifier (e.g., "catalog.namespace.table_name").

    Returns:
        Snapshot: The latest snapshot object, containing summary information about data files and sizes.
    """
    try:
        # Import Iceberg's Java APIs for accessing table metadata
        from org.apache.iceberg import Table
        from org.apache.iceberg.catalog import TableIdentifier
        from org.apache.iceberg.spark import SparkCatalog

        # Get Iceberg catalog and load the table
        catalog = SparkCatalog(spark)
        iceberg_table_obj = catalog.loadTable(TableIdentifier.parse(iceberg_table))

        # Retrieve the latest snapshot
        latest_snapshot = iceberg_table_obj.currentSnapshot()

        return latest_snapshot

    except Exception as e:
        logging.error(f"Failed to retrieve latest snapshot for {iceberg_table}: {e}")
        return None


# Retrieve the latest snapshot id for an Iceberg table
def get_latest_snapshot_id(spark, iceberg_table):
    try:
        # Check if the Iceberg table exists by loading the snapshot metadata
        snapshots_df = spark.read.format("iceberg").load(f"{iceberg_table}.snapshots")

        # Assuming 'snapshot_id' exists, retrieve the latest snapshot based on 'committed_at'
        latest_snapshot = snapshots_df.orderBy(snapshots_df["committed_at"].desc()).first()

        # Return the 'snapshot_id' column
        return latest_snapshot["snapshot_id"] if latest_snapshot else None

    except Exception as e:
        # If the table doesn't exist, return None or handle it appropriately
        print(f"Table '{iceberg_table}' does not exist or cannot be read: {e}")
        return None

# Retrieve the new files between two snapshots
def get_new_files(spark, iceberg_table, pre_snapshot, post_snapshot):
    if not pre_snapshot or not post_snapshot:
        return [], 0

    # Query the manifests for the new snapshot using 'added_snapshot_id'
    manifests_df = spark.read.format("iceberg").load(f"{iceberg_table}.manifests")
    new_manifest_files = manifests_df.filter(manifests_df["added_snapshot_id"] == post_snapshot).select("path", "length")

    # Collect the new files
    new_files = new_manifest_files.select("path").collect()
    total_size = new_manifest_files.select("length").rdd.map(lambda row: row[0]).sum()

    return new_files, total_size