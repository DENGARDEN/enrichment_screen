import subprocess
import os
import shlex
import csv

import pandas as pd


def find_delimiter(filename):
    sniffer = csv.Sniffer()
    with open(filename) as fp:
        delimiter = sniffer.sniff(fp.read(5000)).delimiter
    return delimiter


example_cmd = "mageck mle --count-table RPM_Small_G12D_D9+table.txt --design-matrix Small_G12D_D9+design.txt --output-prefix RPM_Small_G12D_D9 --thread 32 --norm-method total --remove-outliers"
INPUT_PATH = "./kras_readcnt/input"

# Listing files in the target directory
print("Listing read count files to be analyzed...")

waiting_queue = []
for file in os.listdir(INPUT_PATH):

    # For mageck process, prepare txt input files
    if file.endswith(".txt"):
        # TODO: Abstracted design
        date, group, type, frag_no = file.split('+')[0], file.split('+')[1], file.split('+')[2], \
                                     file.split('+')[3].split('.')[0]

        # Find read count table preferentially
        if type == "design":
            continue

        # Validation: Same name of design and count table should be in the input path
        count_file = f"{date}+{group}+table+{frag_no}.txt"
        design_file = f"{date}+{group}+design+{frag_no}.txt"
        if type == "table" and design_file in os.listdir(INPUT_PATH):
            # good to go
            # (design, table)

            # Safety overhead
            if find_delimiter(os.path.join(INPUT_PATH, count_file)) == ',':
                pd.read_csv(os.path.join(INPUT_PATH, count_file), sep=',').to_csv(os.path.join(INPUT_PATH, count_file),
                                                                                  sep='\t',
                                                                                  index=False, quoting=csv.QUOTE_NONE)
            if find_delimiter(os.path.join(INPUT_PATH, design_file)) == ',':
                pd.read_csv(os.path.join(INPUT_PATH, design_file), sep=',').to_csv(
                    os.path.join(INPUT_PATH, design_file),
                    sep='\t',
                    index=False, quoting=csv.QUOTE_NONE)

            waiting_queue.append((os.path.join(INPUT_PATH, design_file), os.path.join(INPUT_PATH, count_file)))

        print(f"Target loaded successfully: {waiting_queue[-1]}")

# Generate child processes for mageck mle

for design, table in waiting_queue:
    sample = design.split('+')[1]
    fragment = design.split('+')[3].split('.')[0]

    cmd_input = shlex.split(
        f"mageck mle --count-table {table} --design-matrix {design} --output-prefix {sample}+{fragment} --thread 32 --norm-method total --remove-outliers")

    process = subprocess.run(cmd_input)
