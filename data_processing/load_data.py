import os
import pandas as pd

# function which load the all csv data in to one csv data
def load_data(folder_path):

    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    if not csv_files:
        print("There are now csv file in the folder")
        return

    # read all csv files and conact into one dataframe
    dfs = []
    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        # replace the space value into NaN
        df = pd.read_csv(file_path)
        df.replace(r'^\s*$', float('nan'), regex=True, inplace=True)
        dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)

    # Data information
    file_size = sum(os.path.getsize(os.path.join(folder_path, file)) for file in csv_files)
    num_rows = combined_df.shape[0]
    num_columns = combined_df.shape[1]
    missing_values = combined_df.isnull().sum().sum()
    missing_values_percentage = (missing_values / (num_rows * num_columns)) * 100
    column_types = combined_df.dtypes

    rows_with_blank_values = combined_df.apply(lambda row: row.str.isspace().any(), axis=1).sum()
    rows_with_blank_values_percentage = (rows_with_blank_values / num_rows) * 100

    combined_df.to_csv('../datasets/combined_data.csv', index=False)

    print("文件大小: {:.2f} bytes".format(file_size))
    print("数据总数: {}".format(num_rows))
    print("缺失值数量: {}".format(missing_values))
    print("缺失值占比: {:.2f}%".format(missing_values_percentage))
    print("含有空白值的行数: {}".format(rows_with_blank_values))
    print("含有空白值的行数占比: {:.2f}%".format(rows_with_blank_values_percentage))
    print("\n每列的数据类型:")
    print(column_types)


if __name__ == "__main__":
    folder_path = "/Users/lixiaoying/Desktop/Masterarbeit/Projekt/data"
    load_data(folder_path)