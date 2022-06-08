"""
This script merges dataset contained in ../dataset 
in one file named dataset.csv
"""

import csv

dataset = open("dataset.csv", "w")
dataset_writer = csv.writer(dataset, delimiter=",")

datasets = ["Benign", "Adware", "Banking", "SMS", "Riskware"]

HEADER_WRITTEN = False

for data in datasets:

    print(f"[*] Adding {data}")

    with open(f"../dataset/{data}.csv") as dataset:
        csv_reader = csv.reader(dataset, delimiter=",")
        
        for i,row in enumerate(csv_reader):

            # write column name only once
            if HEADER_WRITTEN == True and i == 0:
                continue                

            dataset_writer.writerow(row)
