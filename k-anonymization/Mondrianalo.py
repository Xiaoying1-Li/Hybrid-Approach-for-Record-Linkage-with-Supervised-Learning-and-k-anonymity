import csv


RECORDS = []
QIDS = []
K = 0
PARTITIONS = []
ANON_PARTITIONS = []


def importData(path, remove_headers):
    # import data from csv
    with open(path, newline='') as csvfile:
        testreader = csv.reader(csvfile)
        if(remove_headers == "1"):
            # skip header row
            next(testreader)
        for row in testreader:
            # append to list
            RECORDS.append(tuple(row))


