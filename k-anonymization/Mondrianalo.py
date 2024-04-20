import csv
import os

class DataAnonymizer:
    def __init__(self):
        self.records = []
        self.partitions = []
        self.anonymized_partitions = []
        self.k = 0
        self.qids = []

    def load_data(self, path):
        with open(path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header row
            for row in reader:
                self.records.append(tuple(row))

    def select_best_attribute(self, partition, qids):
        attribute_domains = []
        for attrib in qids:
            attrib_values = set()
            for record in partition:
                attrib_values.add(record[attrib])
            attrib_values = sorted(list(attrib_values))
            attribute_domains.append(attrib_values)

        best_index, _ = max(enumerate(map(len, attribute_domains)), key=lambda x: x[1])
        return attribute_domains[best_index], qids[best_index]

    def calculate_frequency_set(self, attribute, attribute_domain, dataset):
        frequencies = []
        for value in attribute_domain:
            count = sum(1 for record in dataset if record[attribute] == value)
            frequencies.append(count)
        return frequencies

    def find_median_value(self, frequencies, attribute_domain):
        total = sum(frequencies)
        median = (total + 1) / 2 if total % 2 != 0 else total / 2
        cumulative_sum = 0
        for idx, freq in enumerate(frequencies):
            cumulative_sum += freq
            if cumulative_sum >= median:
                return attribute_domain[idx]

    def strict_partition(self, attribute, median_value, attribute_domain, dataset):
        lhs = [record for record in dataset if attribute_domain.index(record[attribute]) <= attribute_domain.index(median_value)]
        rhs = [record for record in dataset if attribute_domain.index(record[attribute]) > attribute_domain.index(median_value)]
        return lhs, rhs

    def mondrian_algorithm(self, dataset, sensitive_qids):
        if len(dataset) < 2 * self.k or not sensitive_qids:
            return dataset

        attribute_domain, best_attribute = self.select_best_attribute(dataset, sensitive_qids)
        frequencies = self.calculate_frequency_set(best_attribute, attribute_domain, dataset)
        median_value = self.find_median_value(frequencies, attribute_domain)

        if median_value == attribute_domain[-1]:
            sensitive_qids.remove(best_attribute)
            return self.mondrian_algorithm(dataset, sensitive_qids)

        lhs, rhs = self.strict_partition(best_attribute, median_value, attribute_domain, dataset)

        if len(set(lhs)) < self.k or len(set(rhs)) < self.k:
            sensitive_qids.remove(best_attribute)
            return self.mondrian_algorithm(dataset, sensitive_qids)

        self.partitions.append(self.mondrian_algorithm(lhs, sensitive_qids))
        self.partitions.append(self.mondrian_algorithm(rhs, sensitive_qids))

    def generalize_partition(self, partition, qids):
        anonymized_partition = []
        for record in partition:
            anonymized_record = list(record)
            for idx, qid in enumerate(qids):
                if qid in [3, 7, 9]:
                    min_val = min(map(int, set(record[qid] for record in partition)))
                    max_val = max(map(int, set(record[qid] for record in partition)))
                    anonymized_record[qid] = f"[{min_val}-{max_val}]"
                elif qid == 8:
                    anonymized_record[qid] = record[qid].replace("w", "*")
                else:
                    attribute_values = sorted(list(set(record[qid] for record in partition)))
                    anonymized_record[qid] = attribute_values[0] if len(attribute_values) == 1 else attribute_values
            anonymized_partition.append(tuple(anonymized_record))
        return anonymized_partition

    def write_to_csv(self, dataset, folder_path, filename):
        exclude_columns = [1, 2, 4, 5, 6, 10]
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, dialect='excel')
            for record in dataset:
                modified_record = [record[i] for i in range(len(record)) if i not in exclude_columns]
                writer.writerow(modified_record)

    def anonymize_data(self, args):
        self.k = int(args[2])
        self.qids = list(map(int, args[1].split(",")))

        self.load_data(args[0])
        self.mondrian_algorithm(self.records, self.qids)

        for partition in self.partitions:
            if partition and len(partition) != 0:
                self.anonymized_partitions.append(self.generalize_partition(partition, self.qids))

        k_anonymization = []
        for anonymized_partition in self.anonymized_partitions:
            if len(anonymized_partition) < self.k:
                print("EQUIVALENCE CLASS SIZE LOWER THAN K:", len(anonymized_partition))
                return
            k_anonymization += anonymized_partition

        output_folder = '/Users/lixiaoying/Desktop/Masterarbeit/Master/datasets/datasetsA/'
        if not os.path.exists(output_folder):
            print('the folder doesnt exists ')
        if len(k_anonymization) > 0:
            self.write_to_csv(k_anonymization, output_folder, args[3])
            print("Successful K-anonymization. Output to:", args[3])
        else:
            print("Cannot be anonymized for K =", self.k, "or selected QIDs.")


if __name__ == "__main__":
    anonymizer = DataAnonymizer()
    args = [
        "../datasets/datasetsA/dataset_a.csv",
        "3,7,8,9",
        "3",
        "3anonymization_a.csv"
    ]
    if len(args) != 4:
        print("Incorrect number of arguments to run. Input: input_filename, QID_list, k_value, output_filename")
    else:
        anonymizer.anonymize_data(args)