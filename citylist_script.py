import os
import argparse
import pandas as pd
import pandavro as pdx
import time
import datetime
import logging

def process_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i" , "--input-path", required=True, dest='input' , help="the location of the files to process")
    parser.add_argument("-o" , "--output-path", required=False, dest='output' , help="the location of the output, if not specified it will be written to the location where you ran the script")
    parser.add_argument("-e" , "--error-log", required=False, dest='error' , help="the location of the files errors, if not specified it will be written to the location where you ran the script")
    args = parser.parse_args()
    return args

# Set up logging configuration
def setup_logging(log_file):
    logging.basicConfig(
    filename = log_file,
    level=logging.INFO,
    format = '%(asctime)s|%(levelname)s|%(message)s'
    )

# Load the AVRO file
def load_avro(file_path):
    try:
        file_read = pdx.read_avro(file_path)
        return file_read
    except ValueError as ve:
        logging.error(f"{file_path} is not an Avro file")
    except Exception as e:
        logging.error(f"An error occurred while loading the Avro file {file_path}: {e}")

# Load the JSON file
def load_json(file_path):
    try:
        file_read = pd.read_json(file_path)
        return file_read
    except ValueError as ve:
        logging.error(f"{file_path} is not a Json file")
    except Exception as e:
        logging.error(f"An error occurred while loading the Json file {file_path}: {e}")

# Load the CSV file
def load_csv(file_path):
    try:
        file_read = pd.read_csv(file_path)
        return file_read
    except ValueError as ve:
        logging.error(f"{file_path} is not a CSV/DAT file")
    except Exception as e:
        logging.error(f"An error occurred while loading the CSV/DAT file {file_path}: {e}")
    
# Load the PARQUET file
def load_parquet(file_path):
    try:
        file_read = pd.read_parquet(file_path)
        return file_read
    except ValueError as ve:
        logging.error(f"{file_path} is not a Parquet file")
    except Exception as e:
        logging.error(f"An error occurred while loading the Parquet file {file_path}: {e}") 
    
def verify_file_structure(df1,df2):

    # Check if the columns match
    columns_match = df1.columns.equals(df2.columns)
    
    # Check if the data types match
    dtypes_match = df1.dtypes.equals(df2.dtypes)
    
    # Return True if both match, otherwise False
    return columns_match and dtypes_match
    

    
def main():
    script_start_time = time.time()
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f'Script started')
    args = process_args()
    
    if args.error:
        setup_logging(os.path.join(args.error, f"file_errors_{timestamp}.log"))
    else:
        setup_logging(f"file_errors_{timestamp}.log")
    
    columns_and_types = {
    'Name': 'object',
    'CountryCode': 'object',
    'Population': 'int64' 
    }

    citylist = pd.DataFrame(columns=columns_and_types.keys()).astype(columns_and_types)

    load_start_time = time.time()

    # Iterate over files in the input path
    for filename in os.listdir(args.input):
        if os.path.isfile(os.path.join(args.input, filename)):
            # Get the file extension
            ext = filename.split('.')[-1].upper()  # Get the extension and convert to uppercase
            # Classify based on the extension
            if ext == 'AVRO':
                file_to_df = load_avro(os.path.join(args.input, filename))
                if file_to_df is None:
                    pass
                else:
                    verified_df = verify_file_structure(citylist, file_to_df)
                    if verified_df:
                        citylist = pd.concat([citylist, file_to_df], axis=0).reset_index(drop=True)
                    else:
                        logging.info(f'{filename} is not matching the table structure')
            elif ext in ['CSV', 'DAT']:
                file_to_df = load_csv(os.path.join(args.input, filename))
                if file_to_df is None:
                    pass
                else:
                    verified_df = verify_file_structure(citylist, file_to_df)
                    if verified_df:
                        citylist = pd.concat([citylist, file_to_df], axis=0).reset_index(drop=True)
                    else:
                        logging.info(f'{filename} is not matching the table structure')
            elif ext == 'PARQUET':
                file_to_df = load_parquet(os.path.join(args.input, filename))
                if file_to_df is None:
                    pass
                else:
                    verified_df = verify_file_structure(citylist, file_to_df)
                    if verified_df:
                        citylist = pd.concat([citylist, file_to_df], axis=0).reset_index(drop=True)
                    else:
                        logging.info(f'{filename} is not matching the table structure')
            elif ext == 'JSON':
                file_to_df = load_json(os.path.join(args.input, filename))
                if file_to_df is None:
                    pass
                else:
                    verified_df = verify_file_structure(citylist, file_to_df)
                    if verified_df:
                        citylist = pd.concat([citylist, file_to_df], axis=0).reset_index(drop=True)
                    else:
                        logging.info(f'{filename} is not matching the table structure')
            else:
                logging.info(f"the extention of {filename} is not defined and thus the data was not processed")
            
    load_end_time = time.time()
    load_runtime = load_end_time - load_start_time
    print(f"Load time: {load_runtime:.4f} seconds")

    unique_citylist = citylist.groupby(['Name','CountryCode'])['Population'].mean().reset_index()
    sorted_citylist=unique_citylist.sort_values(by='Name')
    if args.output:
        sorted_citylist.to_csv(os.path.join(args.output, f"combined_data_{timestamp}.csv"), index=False)
    else:
        sorted_citylist.to_csv(f"combined_data_{timestamp}.csv", index=False)


    # finding the number of total rows in all files:
    row_count = len(citylist)
    print(f"The total number of rows in all files is {row_count}")
    
    #finding the number of unique cities in the list
    row_count = len(unique_citylist)
    print(f"The total number of unique rows is {row_count}")

    #findind the city with the largest population:
    largest_population = sorted_citylist['Population'].idxmax()
    print(f"The city with the largest population is: {sorted_citylist.loc[largest_population, 'Name']}")

    #find the total population in all the cities in Brazil:
    brazil_population = sorted_citylist.loc[sorted_citylist['CountryCode'] == 'BRA']['Population'].sum()
    print(f"The total population in all the cities in Brazil is: {brazil_population.sum()}")

    script_end_time = time.time()
    script_runtime = script_end_time - script_start_time
    print(f"Runtime: {script_runtime:.4f} seconds")
    
    print(f'Script ended')



if __name__ == '__main__':
    main()
