import numpy as np
import pandas as pd
import os
from datetime import datetime



class WrongFile(Exception):
    def __init__(self, filename, message="is not a CSV"):
        self.filename = filename
        self.message = f"filename {message}"
        super().__init__(self.message)

def Clean_Save_Csv(input, output):
    """ Check if there is any NaN value in the (input) provided 
        by the user and return the file cleaned. Save the cleaned
        file with the desired name (output) if possible. """
    
    # Access to go take and save the file in a new folder
    input_folder = "raw data"
    output_folder = "data_cleaned"
    
    # Complete file path with the os library
    input_file_path = os.path.join(input_folder, input)
    output_file_path = os.path.join(output_folder, output)
    
    try:
        # Import the CSV file
        df = pd.read_csv(input_file_path)
        
        #Check the NaN values in the file
        if df.isna().any().any():
            print(f"NaN value found in {input} and deleted")
            df_cleaned = df.dropna()
        else:
            print("No NaN values found")
            df_cleaned = df 
        
        try:
            # Attempt to save the cleaned DataFrame
            df_cleaned.to_csv(output_file_path, index=False)
            print(f"File {output} cleaned and saved with success")
        except Exception as e:
            raise WrongFile(output, f"Error saving {output}: {e}")

        return df_cleaned
    
    # Handle errors
    except FileNotFoundError:
        print(f"Error: File {input_file_path} not found")

    except pd.errors.EmptyDataError:
        print(f"Error: {input_file_path} is empty")

def Merge_Csv_Files(file1, file2, output_file):
    """ Merge two files (file1, file2) into a new one (output_file)"""
    
    # Acces to go take and save the files in a new folder
    file1_folder = "raw data"
    file2_folder = "raw data"
    output_file_folder = "data_cleaned"
    
    # Full file path
    file1_full_path = os.path.join(file1_folder, file1)
    file2_full_path = os.path.join(file2_folder, file2)
    output_file_full_path = os.path.join(output_file_folder, output_file)
    
    try:
        df1 = pd.read_csv(file1_full_path)
        df2 = pd.read_csv(file2_full_path)
        merged_df = pd.concat([df1, df2], ignore_index=True)
    
        # Check if the file has been saved
        if output_file:
            merged_df.to_csv(output_file_full_path, index=False)
            print(f"Fusion of {file1} and {file2} succeeded. {output_file} registered with success.")
        else:
            print(f"Error {file1} and {file2} haven't been merged")
            
        return merged_df

    except pd.errors.EmptyDataError:
        if df1.empty:
            print(f"Empty or no data found in {file1}.")
        else:
            print(f"File {file1}.")
        if df2.empty:
            print(f"Empty or no data found in {file2}.")
        else:
            print(f"File {file2}.")
    
def Merge_Csv_Same_Columns(file1, file2, output_file=None):
    """ Merge files based on defined columns """
     # Acces to go take and save the files in a new folder
    file1_folder = "raw data"
    file2_folder = "raw data"
    output_file_folder = "data_cleaned"
        
    # Full file path
    file1_full_path = os.path.join(file1_folder, file1)
    file2_full_path = os.path.join(file2_folder, file2)
    output_file_full_path = os.path.join(output_file_folder, output_file)
    
    try:
        df1 = pd.read_csv(file1_full_path)
        df2 = pd.read_csv(file2_full_path)

        Columns = ['Manufacturer', 'Modèle', 'engine', 'fuel', 'Max_power', 'transmission', 'year', 'selling_price', 'Selling_price_usd', 'km_driven', 'Car_age', 'mileage', 'seats']

        # Check if the Columns are in both dataFrame
        if not set(Columns).issubset(df1.columns) or not set(Columns).issubset(df2.columns):
            print(f"One or more required columns are missing in {file1} or {file2}.")
        else:
            # Create the columns
            df1 = df1[Columns]
            df2 = df2[Columns]
            
            # Fusion of the 2 data frames
            df_complet = [df1, df2]
            df = pd.concat(df_complet, sort=True)

            # Save the data
            if output_file:
                df.to_csv(output_file_full_path, index=False)
                print(f"Fusion of {file1} and {file2} succeeded. {output_file} registered with success.")
            else:
                print("Saving Error: No output file specified.")

            return df

    except pd.errors.EmptyDataError:
        if df1.empty:
            print(f"Empty or no data found in {file1_full_path}.")
        else:
            print(f"File {file1_full_path}.")
        if df2.empty:
            print(f"Empty or no data found in {file2_full_path}.")
        else:
            print(f"File {file2_full_path}.")

def read_Csv_Jupyter(input_file):
    """Read the CSV file in jupyter notebook"""
    folder = "data_cleaned"
    file_path = os.path.join(folder, input_file)

    df = pd.read_csv(file_path)
    
    #Add Modele column
    df['Modele'] = df['name'].str.split(n=1).str[1]
    
    #Add current age
    current_year = datetime.now().year
    df['Car_age'] = current_year - df['year']
    
    exchange_rate = 84
    # Convert price in USD
    df['Selling_price_usd'] = df['selling_price'] / exchange_rate

    # Manufacturer alone
    df['Manufacturer'] = df['name'].str.split(' ').str[0]
    
    column_order = ['Manufacturer', 'Modele', 'engine', 'fuel', 'max_power', 'transmission', 'year', 'selling_price', 'Selling_price_usd', 'km_driven', 'Car_age', 'mileage', 'seats']
    df_new = df[column_order]
    df_new.columns = [col.upper() for col in df_new.columns]
    
    # New dataFrame with the following columns
    column_order = ['Manufacturer', 'Modele', 'engine', 'fuel', 'max_power', 'Selling_price_usd', 'transmission', 'year', 'selling_price', 'km_driven', 'Car_age', 'mileage', 'seats']
    df_new = df[column_order]  
    
    # All the dataFrame in uppercases
    df_new.columns = [col.upper() for col in df_new.columns]

    return df_new

def read_directory(csv_dir):
    """Check if the files in the folder end with .csv"""
    if os.path.isdir(csv_dir):
        non_csv_files = [file for file in os.listdir(csv_dir) if not file[-4:] == ".csv"]
        if non_csv_files:
            print("Files that are not in CSV format:")
            print("\n".join(non_csv_files))
        else:
            print("All files are in CSV format.")
    else:
        raise WrongFile(filename=csv_dir, message="is not a valid directory")


def Car_By_Brand_And_Year(df_new,year, Manufacturer):
    """En fonction de l'année et du fabricant, renvoie la voiture la plus abordable"""
    
    car_columns = df_new[
        ["FUEL", "MANUFACTURER", "YEAR", "KM_DRIVEN", "TRANSMISSION", "SELLING_PRICE"]
    ][
        (df_new["YEAR"] == year) & (df_new["MANUFACTURER"] == Manufacturer)
    ]
    car_selected = car_columns.sort_values(by="SELLING_PRICE", ascending=True)
    return car_selected
