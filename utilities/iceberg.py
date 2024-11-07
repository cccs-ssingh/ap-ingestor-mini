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
