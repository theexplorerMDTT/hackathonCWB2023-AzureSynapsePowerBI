import subprocess
import os

# Specify the path to the MDB file
mdb_file = "/Users/yuko/Documents/DATA-PROJECTS/Hackathon-VertexVenture/AzData-EnrollmentDB/ENROLL2022_20221114.mdb"

# Extract the directory path from the MDB file
mdb_directory = os.path.dirname(mdb_file)

# Create the output folder within the directory path if it doesn't exist
output_folder = os.path.join(mdb_directory, "Output-CSV")
os.makedirs(output_folder, exist_ok=True)

# Get a list of table names in the MDB file
cmd_get_tables = f"mdb-tables -1 {mdb_file}"
tables = subprocess.check_output(cmd_get_tables, shell=True, text=True).splitlines()

# Loop through each table and export it to CSV
for table_name in tables:
    print(table_name)


    if not table_name:
        continue  # Skip empty table names

    # Generate the output CSV file path based on the table name
    file_name = table_name.replace('/', '')  # Remove slash
    csv_file = os.path.join(output_folder, f"{file_name.replace(' ', '_')}.csv")

    # Export the table to CSV
    cmd_export_table = f"mdb-export '{mdb_file}' '{table_name}' > '{csv_file}'"
    subprocess.run(cmd_export_table, shell=True)

    print(f"Exported table: {table_name}")

print("Conversion completed successfully.")
