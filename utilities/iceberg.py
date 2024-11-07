import logging

# Function to inspect the snapshots table
def inspect_snapshots_table(spark, iceberg_table):
    # Load the snapshots table
    snapshots_df = spark.read.format("iceberg").load(f"{iceberg_table}.snapshots")

    # Show all columns and data for debugging
    snapshots_df.show(truncate=False)

    # Print the available columns in the snapshots table
    print("Available columns in snapshots table:", snapshots_df.columns)

# Retrieve the latest snapshot for an Iceberg table
def get_latest_snapshot(spark, iceberg_table):
    try:
        # Check if the Iceberg table exists by loading the snapshot metadata
        snapshots_df = spark.read.format("iceberg").load(f"{iceberg_table}.snapshots")

        # Assuming 'snapshot_id' exists, retrieve the latest snapshot based on 'committed_at'
        latest_snapshot = snapshots_df.orderBy(snapshots_df["committed_at"].desc()).first()

        # Return the 'snapshot_id' column
        return latest_snapshot if latest_snapshot else None

    except Exception as e:
        # If the table doesn't exist, return None or handle it appropriately
        print(f"Table '{iceberg_table}' does not exist or cannot be read: {e}")
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