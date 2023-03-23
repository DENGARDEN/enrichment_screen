import subprocess
import os
import shlex
import csv

import pandas as pd

INPUT_PATH = "./kras_rpm/input"
RESULT_PATH = "./kras_rpm/result"


def find_delimiter(filename):
    sniffer = csv.Sniffer()
    with open(filename) as fp:
        delimiter = sniffer.sniff(fp.read(5000)).delimiter
    return delimiter

    """
        os.system('mageck test -k ./GX19_Mageck/{a}_{b}_GX19_Mageck_input.txt'.format(a = dataset,b = rep,c= groupnum)+
        ' -c 0 -t 1 --norm-method median' +' -n GX19_Mageck/'+
        '{a}_{b}_GX19_Mageck_{c}_group'.format(a = dataset,b = rep,c = groupnum))
        
    """


# Listing files in the target directory
print("Listing read count files to be analyzed...")

waiting_queue = []
for file in sorted(os.listdir(INPUT_PATH)):

    # For mageck process, prepare txt input files
    if file.endswith(".txt"):
        # TODO: Abstracted design
        date, group, rep, type = file.split("+")

        # Find read count table preferentially
        if type == "design.txt":
            continue

        # Validation: Same name of design and count table should be in the input path
        count_file = f"{date}+{group}+{rep}+table.txt"
        design_file = f"{date}+{group}+{rep}+design.txt"
        if type == "table.txt" and design_file in os.listdir(INPUT_PATH):
            # good to go
            # (design, table)

            # Safety overhead
            if find_delimiter(os.path.join(INPUT_PATH, count_file)) == ",":
                pd.read_csv(os.path.join(INPUT_PATH, count_file), sep="\t").to_csv(
                    os.path.join(INPUT_PATH, count_file),
                    sep="\t",
                    index=False,
                    quoting=csv.QUOTE_NONE,
                )
            if find_delimiter(os.path.join(INPUT_PATH, design_file)) == ",":
                pd.read_csv(os.path.join(INPUT_PATH, design_file), sep="\t").to_csv(
                    os.path.join(INPUT_PATH, design_file),
                    sep="\t",
                    index=False,
                    quoting=csv.QUOTE_NONE,
                )

            waiting_queue.append(
                (
                    os.path.join(INPUT_PATH, design_file),
                    os.path.join(INPUT_PATH, count_file),
                )
            )

        print(f"Target loaded successfully: {waiting_queue[-1]}")

# Generate child processes for mageck mle

for design, table in waiting_queue:
    sample = "/".join([RESULT_PATH, f'{design.split("+")[1]+design.split("+")[2]}'])
    fragment = design.split("+")[-1].split(".")[0]

    cmd_input = shlex.split(
        f"mageck test --count-table {table} -c 0 -t 1 --norm-method median --output-prefix {sample}"
    )

    process = subprocess.run(cmd_input)
