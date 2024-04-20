import pandas as pd
import os
import random

# Function which try to set the value into NaN value in the combined_data
def clean_data(combined_data_file, cleaned_data_file):
    data = pd.read_csv(combined_data_file)

    # handle the row with NaN value
    for index, row in data.iterrows():
        if row.isnull().any():
            soc_soe_id = row[' soc_sec_id']
            # find the rows with the same soc_sec_id
            same_soc_soe_id_rows = data[data[' soc_sec_id'] == soc_soe_id]
            # if soc_sec_id is unique, delete dircetly如
            if len(same_soc_soe_id_rows) == 1:
                data = data.drop(index)
            else:
                # find the column with NaN in the same soc_sec_id
                nan_columns = row[row.isnull()].index.tolist()
                for column in nan_columns:
                    # find other column with same soc_sec_id
                    other_values = same_soc_soe_id_rows[column].dropna()
                    if len(other_values) == 0:
                        # id other column also NaN in the same soc_sec_id, delete all relevant rows
                        data = data[data[' soc_sec_id'] != soc_soe_id]
                        break
                    else:
                        # find the value with most frequency
                        most_common_value = other_values.mode()[0]
                        # replace NaN with most frequency value
                        data.at[index, column] = most_common_value

    data.to_csv(cleaned_data_file, index=False)
    print('data cleaning is finshed')

# Function of merging the febrl data with adult data, for sure that same soc_sec_id matching the same adult data
def merge_data(cleaned_data_file, adult_data_file, merged_data_file):

    cleaned_data = pd.read_csv(cleaned_data_file)
    adult_data = pd.read_csv(adult_data_file)

    # put soc_sec_id into two lists: unique and ununique
    unique_ids = cleaned_data[' soc_sec_id'].unique().tolist()
    ununique_ids = cleaned_data[cleaned_data.duplicated(subset=[' soc_sec_id'], keep=False)][
        ' soc_sec_id'].unique().tolist()

    # for soc_sec_id in the unique list, conact random with adult data
    for soc_sec_id in unique_ids:
        random_adult_data = adult_data.sample(n=1)
        cleaned_data.loc[cleaned_data[' soc_sec_id'] == soc_sec_id, adult_data.columns] = random_adult_data.values

    # for soc_sec_id in the ununique list sort at the first
    ununique_ids.sort()

    # initie pointers j and i
    i = 0
    j = 0

    while i < len(ununique_ids) and j < len(adult_data):
        soc_sec_id = ununique_ids[i]
        # if i points the first soc_sec_id, conact one adult data randomly
        if i == 0:
            random_adult_data = adult_data.sample(n=1)
            cleaned_data.loc[cleaned_data[' soc_sec_id'] == soc_sec_id, adult_data.columns] = random_adult_data.values
            j += 1
        else:
            # if i not point the first soc_sec_id
            prev_soc_sec_id = ununique_ids[i - 1]
            if soc_sec_id == prev_soc_sec_id:
                # if i = the prev soc_sec_id, put the data that j point in to data of soc_sec_id
                cleaned_data.loc[cleaned_data[' soc_sec_id'] == soc_sec_id, adult_data.columns] = adult_data.iloc[
                    j].values
            else:
                # if i =! prev soc_sec_id, random select adult data
                random_adult_data = adult_data.sample(n=1)
                cleaned_data.loc[
                    cleaned_data[' soc_sec_id'] == soc_sec_id, adult_data.columns] = random_adult_data.values
                j += 1
        i += 1

    # delete the adult data, which doesn't conact with any soc_sec_id
    cleaned_data = cleaned_data.dropna(subset=[' soc_sec_id'])

    cleaned_data.to_csv(merged_data_file, index=False)
    print('data merging is finished')

# function which processes the merged data
def process_merged_data(merged_data_file, processed_data_file):
    merged_data = pd.read_csv(merged_data_file)
    #delete the space in the colums
    merged_data.columns = merged_data.columns.str.strip()
    columns_to_drop = ['rec_id', 'ID', 'sex', 'age', 'race', 'marital-status', 'education', 'native-country',
                       'workclass', 'occupation']
    merged_data = merged_data.drop(columns=columns_to_drop, errors='ignore')
    # delete the postcode which don't have length of 4
    merged_data = merged_data[merged_data['postcode'].astype(str).str.len() == 4]
    # change the datatype
    merged_data['street_number'] = merged_data['street_number'].astype(int)
    merged_data['date_of_birth'] = merged_data['date_of_birth'].astype(int)
    merged_data.to_csv(processed_data_file, index=False)
    print('data process is finished')

# function which splite the data into two csv files
def split_and_remove_duplicates(processed_data_file, dataset_a_file, dataset_b_file):
    processed_data = pd.read_csv(processed_data_file)

    # Split data into two equal parts
    num_rows = processed_data.shape[0]
    half_num_rows = num_rows // 2
    dataset_a = processed_data.iloc[:half_num_rows].copy()
    dataset_b = processed_data.iloc[half_num_rows:].copy()

    # Remove duplicates from dataset A and B
    dataset_a = dataset_a.drop_duplicates(subset='soc_sec_id', keep=False)
    dataset_b = dataset_b.drop_duplicates(subset='soc_sec_id', keep=False)

    # Add index columns with suffixes _a and _b
    dataset_a.insert(0, 'index', [str(i) + '_a' for i in range(1, 1 + len(dataset_a))])
    dataset_b.insert(0, 'index', [str(i) + '_b' for i in range(1, 1 + len(dataset_b))])

    # Save datasets A and B to CSV files
    dataset_a.to_csv(dataset_a_file, index=False)
    dataset_b.to_csv(dataset_b_file, index=False)
    print('data is spliting into two csvs and duplicates is removed')

# function to add randomly noise of the data
def add_noise_to_data(data_file, target_column):
    data = pd.read_csv(data_file)

    # calculate the percentage of  rows that need to add noise
    num_rows = data.shape[0]
    num_rows_to_add_noise = int(0.2 * num_rows)

    # randomly select rows
    rows_to_add_noise = random.sample(range(num_rows), num_rows_to_add_noise)

    for row_index in rows_to_add_noise:
        current_value = data.loc[row_index, target_column]

        # if value length>3, added noise
        if isinstance(current_value, str) and len(current_value) > 3:
            # randomly select method to add noise: delete or add string
            noise_choice = random.choice(["delete", "add"])

            if noise_choice == "delete":
                delete_index = random.randint(0, len(current_value) - 1)
                new_value = current_value[:delete_index] + current_value[delete_index + 1:]

            elif noise_choice == "add":
                insert_index = random.randint(0, len(current_value))
                insert_char = chr(random.randint(97, 122))  # 随机选择小写字母
                new_value = current_value[:insert_index] + insert_char + current_value[insert_index:]

            data.loc[row_index, target_column] = new_value

    data.to_csv(data_file, index=False)
    print('data is added by noise')

if __name__ == "__main__":
    combined_data_file = "../datasets/combined_data.csv"
    cleaned_data_file = "../datasets/cleaned_data.csv"
    adult_data_file = "../datasets/adult.csv"
    merged_data_file = "../datasets/merged_data.csv"
    processed_data_file = "../datasets/processed_data.csv"
    dataset_a_file = "../datasets/datasetsA/dataset_a.csv"
    dataset_b_file = "../datasets/datasetsB/dataset_b.csv"

    clean_data(combined_data_file, cleaned_data_file)
    merge_data(cleaned_data_file,adult_data_file,merged_data_file)
    process_merged_data(merged_data_file, processed_data_file)
    split_and_remove_duplicates(processed_data_file, dataset_a_file, dataset_b_file)
    add_noise_to_data(dataset_a_file, ['given_name', 'surname', 'address_1', 'address_2', 'suburb', 'state'])
    add_noise_to_data(dataset_b_file, ['given_name', 'surname', 'address_1', 'address_2', 'suburb', 'state'])


