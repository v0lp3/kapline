"""
This script uses quark to convert .apk files into a features vector
"""

import os
import subprocess
import json
import csv

DATASET_PATH = "../dataset"
REPORTS_FOLDER = "./reports"
RULES_PATH = "~/.quark-engine/quark-rules/rules"


def generate_reports():
    """
    We use quark engine to generate a report that describe all the actions
    performed by the application once installed

    This function saves the report in a folder into REPORTS_FOLDER which
    represents the malware's family.

    """

    proc = []

    if not os.path.exists(REPORTS_FOLDER):
        os.mkdir(REPORTS_FOLDER)

    for family in os.listdir(DATASET_PATH):

        if family in ["README.md", "dataset.csv"]:
            continue

        report_family_path = os.path.join(REPORTS_FOLDER, family)

        if not os.path.exists(report_family_path):
            os.mkdir(os.path.join(REPORTS_FOLDER, family))

        for apk in os.listdir(os.path.join(DATASET_PATH, family)):

            apk_path = os.path.join(DATASET_PATH, family, apk)
            report_path = os.path.join(report_family_path, f"{apk}.json")

            if os.path.exists(report_path):
                # already analyzed, skip
                continue

            proc.append(
                subprocess.Popen(["quark", "-a", apk_path, "-o", report_path]),
            )

            if len(proc) > 2:
                for i in proc:
                    i.wait()

                proc.clear()


def filter_crime(crime):
    """
    Parse a crime: identify the matched rule number and the score
    """

    rule = int(crime["rule"].split(".json")[0])
    weight = crime["weight"]

    return (rule, weight)


def extract_features(filepath: str) -> list():
    """
    Extracts all features from a single file.
    """

    with open(filepath, "r") as application:
        report = json.load(application)

    features = [filter_crime(crime) for crime in report["crimes"]]

    features.sort(key=lambda x: x[0])

    return [crime_score for crime_score in features]


def generate_vectors_dataset():

    """
    This function generate a csv file that contains all the features vectors
    of the dataset apk.
    """

    rules_number = len(os.listdir(RULES_PATH))

    header = ["apk", "family"] + [i for i in range(1, rules_number)]

    with open(
        os.path.join(DATASET_PATH, "dataset.csv"), "w", encoding="UTF8"
    ) as dataset:
        writer = csv.writer(dataset)

        writer.writerow(header)

        for family in os.listdir(REPORTS_FOLDER):
            for report in os.listdir(os.path.join(REPORTS_FOLDER, family)):

                features = extract_features(
                    os.path.join(REPORTS_FOLDER, family, report),
                )

                writer.writerow([report.split(".json")[0], family] + features)


generate_reports()

generate_vectors_dataset()

# remove useless files

os.system(f"rm -r {REPORTS_FOLDER}")

exit(0)
