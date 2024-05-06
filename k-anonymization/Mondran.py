import argparse
import copy
import csv
import os
import sys
import pandas as pd


QUASI_ID_INDEX = None
RESULT1 = []
RESULT2 = []
args = sys.argv
default_input_file = "../datasets/datasetsA/dataset_a.csv"
default_output_file = r"output/output_dataset_a.csv"
default_quality_index = [3, 7, 8, 9]
default_k = 3

class FileHandle(object):
    def __init__(self):
        pass

    @staticmethod
    def read_source_data(file):
        """
        read data
        :param file: link to the input file
        """
        with open(file, "r") as fr:
            data = fr.readlines()
        new_data = [tuple(item.strip().split(",")) for item in data if item.strip()]
        data_drop_header = new_data[1:]
        return data_drop_header

    @staticmethod
    def dump_result(dataset, output_filename):
        """
        write result into the file
        :param dataset: datasets after anonymization
        :param output_filename: link to the output file
        """
        headers = ["index","street_number", "postcode", "state", "date_of_birth","salary-class"]
        exclude_cols = [1, 2, 4, 5, 6, 10]
        with open(output_filename, "w", newline="", encoding="utf-8-sig") as fout:
            writer = csv.writer(fout, dialect='excel')
            writer.writerow(headers)
            for data in dataset:
                modify_row = [data[i] for i in range(len(data)) if i not in exclude_cols]
                writer.writerow(modify_row)


class Process(object):
    def __init__(self,  **kwargs):
        self.input_file = kwargs.get("input_file")
        self.output_file = kwargs.get("output_file")
        self.k = kwargs.get("k")
        attribute_index = kwargs.get("attribute_index")
        self.attribute_index = [int(item) for item in attribute_index]


    def select_attribute_best_value(self, dataset, attribute_index):
        """
        Selects the best value of an attribute to return,
        and returns a list of the most diverse attribute values after de-duplication, and the attribute index.
        """
        result = []
        length = []
        for attribute in attribute_index:
            attribute_list = [data[attribute] for data in dataset]
            attribute_list = list(set(attribute_list))
            attribute_list.sort()  # todo 这里是否考虑对数值进行int转换之后，再排序

            length.append(len(attribute_list))
            result.append(list(attribute_list))

        max_length = max(length)
        max_length_index = length.index(max_length)

        attribute = attribute_index[max_length_index]
        attribute_value_list = result[max_length_index]
        return attribute_value_list, attribute

    def get_frequency(self, dataset, attribute_index, attribute_value):
        """
        get frequency for the attribute
        :param dataset: original dataset
        :param attribute_index: index of the attribute, from select_attribute_best_value
        :param attribute_value: list for the value of attribute, from select_attribute_best_value
        """
        result = []
        data_length = len(attribute_value)
        for attr in range(data_length):
            this_columns = [item[attribute_index] for item in dataset]
            result.append(this_columns.count(attribute_value[attr]))

        return result

    def get_median(self, frequency_list, attribute_value):
        """
        calculate the median value
        :param frequency_list: list of the frequency
        :param attribute_value: list of the attribute, from select_attribute_best_value
        """
        frequency_sum = sum(frequency_list)
        media = (frequency_sum+1)/2 if frequency_sum % 2 != 0 else int(((frequency_sum/2+1) + frequency_sum/2) / 2)

        default_val = -1
        for i, j in enumerate(frequency_list):
            media = media - j
            if 0 >= media:
                default_val = i
                return attribute_value[default_val]
        return attribute_value[default_val]

    def mondrian_process(self, dataset, attribute_index, k):
        """
        Mondrian algorithm
        """
        if any([len(dataset) < 2*k, len(attribute_index) == 0]):
            return dataset

        attribute_value_list, attribute = self.select_attribute_best_value(dataset, attribute_index)
        frequency_list = self.get_frequency(dataset, attribute, attribute_value_list)
        media_attr_index = self.get_median(frequency_list, attribute_value_list)
        split_point = attribute_value_list.index(media_attr_index)

        # split the data
        data1 = [item for item in dataset if attribute_value_list.index(item[attribute]) <= split_point]
        data2 = [item for item in dataset if attribute_value_list.index(item[attribute]) > split_point]

        data1_len = len(set(data1))
        data2_len = len(set(data2))
        if any([data1_len < k, data2_len < k]):
            tmp_attribute = copy.copy(attribute_index)
            tmp_attribute.remove(attribute)
            return self.mondrian_process(dataset, tmp_attribute, k)

        data1_res = self.mondrian_process(data1, attribute_index, k)
        data2_res = self.mondrian_process(data2, attribute_index, k)
        RESULT1.append(data1_res)
        RESULT1.append(data2_res)

    def anonymise(self, partdata, attribute_index):
        """
        Mondrian anonymization
        """
        summary = list()

        for attr in attribute_index:
            attr_list = [item[attr] for item in partdata]
            attr_set = list(set(attr_list))
            if attr in [3, 7, 9]:
                minmum_val = min(map(int, attr_set))
                maxmum_val = max(map(int, attr_set))
                summary.append("[%s-%s]" % (minmum_val, maxmum_val))
            elif attr == 8:
                # Merge all the data in this group as a list option
                state_data = [item.strip() for item in attr_set]
                state_data = list(set(state_data))
                state_data.sort()
                summary.append(state_data)
            else:
                if len(attr_set) > 1:
                    attr_list.sort()
                    summary.append(attr_set)
                else:
                    summary.append(attr_set[0])

        anon_data = list()
        for data in partdata:
            __tmp = list(data)
            for j, q in enumerate(attribute_index):
                __tmp[q] = summary[j]
            anon_data.append(__tmp)

        return anon_data

    def main(self):
        global QUASI_ID_INDEX
        QUASI_ID_INDEX = self.attribute_index

        file_handle = FileHandle()
        # read data
        dataset = file_handle.read_source_data(input_file)
        # implemente algorithm
        self.mondrian_process(dataset, self.attribute_index, k)

        # anonymizaion
        for partdata in RESULT1:
            if partdata:
                RESULT2.extend(self.anonymise(partdata, self.attribute_index))

        # check data for K format
        for data in RESULT2:
            if len(data) < k:
                print(f"k value is too big，dataset can not spilt: {len(data)}")
                return

        if RESULT2:
            file_handle.dump_result(RESULT2, output_file)
            print(f"data anonymization is successful, the address to the outputfile：{output_file}")
        else:
            print(f"dataset can not be anonymized数, k:{k}, input_quality:{QUASI_ID_INDEX}")


if __name__ == '__main__':


    if len(args) > 1:
        # python demo.py --input_file dataset_a.csv --output_file output_a.csv --quality_index 3,7,8,9 --k 3
        parser = argparse.ArgumentParser()
        parser.add_argument('--input_file', required=True, help='input the input_file path.')
        parser.add_argument('--output_file', required=True, help='input the output_file path.')
        parser.add_argument('--quality_index', required=True, help='input the quality_index, if has more, please split , '
                                                                   'for example: 0,1,2,3.')
        parser.add_argument('--k', required=True, help='input the K')
        args = parser.parse_args()
        input_file = args.input_file
        output_file = args.output_file
        quality_index = args.quality_index.split(",")
        quality_index = [int(item.strip()) for item in quality_index]
        k = int(args.k)
    else:
        input_file = default_input_file
        output_file = default_output_file
        quality_index = default_quality_index
        k = default_k

    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    main = Process(input_file=input_file, attribute_index=quality_index, k=k, output_file=output_file)
    main.main()

