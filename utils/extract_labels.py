from copy import copy
import os
import json

QUARK_PATH = os.path.expanduser("~") + "/.quark-engine/quark-rules"

"""
Load label associated to quark-rules
"""

action_labels = {}


for rule in os.listdir(os.path.join(QUARK_PATH, "rules")):
    with open(os.path.join(QUARK_PATH, "rules", rule)) as r:
        tmp = json.load(r)

    for i in tmp["label"]:
        if i not in action_labels:
            action_labels[i] = []

        action_labels[i].append(int(rule.split(".json")[0]))

action_labels_tmp = copy(action_labels)

for label, v in action_labels_tmp.items():
    if len(v) < 5:
        action_labels.pop(label)

with open("quark_labels.json", "w") as f:
    json.dump(action_labels, f)
