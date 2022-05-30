"""
This script uses quark to convert .apk files into a features vector
"""

import os


DATASET_PATH = "../dataset"
REPORTS_FOLDER = "./reports"


if not os.path.exists(REPORTS_FOLDER):
    os.mkdir(REPORTS_FOLDER)

for family in os.listdir(DATASET_PATH):

    if family == "README.md":
        continue

    report_family_path = os.path.join(REPORTS_FOLDER, family)

    if not os.path.exists(report_family_path):
        os.mkdir(os.path.join(REPORTS_FOLDER, family))

    for apk in os.listdir(os.path.join(DATASET_PATH, family)):

        apk_path = os.path.join(DATASET_PATH, family, apk)
        report_path = os.path.join(report_family_path, f"{apk}.json")

        os.system(
            "quark -a {} -o {}".format(
                apk_path,
                report_path,
            ),
        )
