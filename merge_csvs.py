import pandas as pd
import os
import glob

def merge_csv_files(output_file_name):
    # Use glob to find all CSV files in the current directory
    csv_files = glob.glob("blockgroup_2011_5yr_part_*.csv")

    # List to hold the DataFrames
    df_list = []

    # Iterate over the list of CSV files
    for file in csv_files:
        # Read each CSV file into a DataFrame
        df = pd.read_csv(file)
        # Append the DataFrame to the list
        df_list.append(df)

    # Concatenate all DataFrames in the list into a single DataFrame
    merged_df = pd.concat(df_list, ignore_index=True)

    # Write the merged DataFrame to a single CSV file
    merged_df.to_csv(output_file_name, index=False)
    print(f"Merged {len(csv_files)} files into {output_file_name}")

# Call the function to merge the CSV files
merge_csv_files("blockgroup_2011_5yr.csv")
