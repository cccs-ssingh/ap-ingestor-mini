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