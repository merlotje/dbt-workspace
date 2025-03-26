import os
import re
import pandas as pd


input_folder = r"C:\Users\margot.verheij\Documents\ANWB\input"
output_folder = r"C:\Users\margot.verheij\Documents\ANWB\cleansed"


def replace_special_characters(column_name):
    """
    Replace all special characters and spaces with underscores in a column name, so that it fits Bigquery standard of allowed characters.
    """
    return re.sub(r'[^a-zA-Z0-9]', '_', column_name)


def transform(input_file, output_file):
    """
    Read a CSV file, clean the column headers, and save the cleaned version.
    """
    df = pd.read_csv(input_file)
    df.columns = [replace_special_characters(col) for col in df.columns]
    df.to_csv(output_file, index=False)
    print(f"Processed file saved as: {output_file}")


def loop_files_in_folder(input_folder, output_folder):
    """
    Process all CSV files in the specified input folder and save them with cleaned headers.
    """
    os.makedirs(output_folder, exist_ok=True)
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".csv"):
            input_file = os.path.join(input_folder, file_name)
            output_file = os.path.join(output_folder, file_name)
            transform(input_file, output_file)


loop_files_in_folder(input_folder, output_folder)
