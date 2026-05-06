import glob
import os

def hadoop(input_folder, output_folder, mapper_fn, reducer_fn):

    def read_records_from_input(input_folder):
        sequence = []
        files = glob.glob(f"{input_folder}*")
        for file in files:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence

    def save_results_to_output(result):
        with open(f"{output_folder}part-00000", "w", encoding="utf-8") as f:
            for key, value in result:
                f.write(f"{key}\t{value}\n")

    def create_success_file(output_folder):
        with open(os.path.join(output_folder, "_SUCCESS"), "w", encoding="utf-8") as f:
            f.write("")

    def create_output_directory(output_folder):
        if os.path.exists(output_folder):
            raise FileExistsError(f"The folder '{output_folder}' already exists.")
        else:
            os.makedirs(output_folder)

    sequence = read_records_from_input(input_folder)
    pairs_sequence = mapper_fn(sequence)
    pairs_sequence = sorted(pairs_sequence)
    result = reducer_fn(pairs_sequence)
    create_output_directory(output_folder)
    save_results_to_output(result)
    create_success_file(output_folder)
    # pylint: disable=import-error

from .mapreduce import hadoop as run_mapreduce_job  # type: ignore

#
# Columns:
# total_bill, tip, sex, smoker, day, time, size
#


#
# SELECT *, tip/total_bill as tip_rate
# FROM tips;
#
def mapper_query_1(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip() + ",tip_rate"))
        else:
            row_values = row.strip().split(",")
            total_bill = float(row_values[0])
            tip = float(row_values[1])
            tip_rate = tip / total_bill
            result.append((index, row.strip() + "," + str(tip_rate)))
    return result


def reducer_query_1(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE time = 'Dinner';
#
def mapper_query_2(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner":
                result.append((index, row.strip()))
    return result


def reducer_query_2(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE time = 'Dinner' AND tip > 5.00;
#
def mapper_query_3(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner" and float(row_values[1]) > 5.00:
                result.append((index, row.strip()))
    return result


def reducer_query_3(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE size >= 5 OR total_bill > 45;
#
def mapper_query_4(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if int(row_values[6]) >= 5 or float(row_values[0]) > 45:
                result.append((index, row.strip()))
    return result


def reducer_query_4(sequence):
    """Reducer"""
    return sequence


#
# SELECT sex, count(*)
# FROM tips
# GROUP BY sex;
#
def mapper_query_5(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue
        row_values = row.strip().split(",")
        result.append((row_values[2], 1))
    return result


def reducer_query_5(sequence):
    """Reducer"""
    counter = dict()
    for key, value in sequence:
        if key not in counter:
            counter[key] = 0
        counter[key] += value
    return list(counter.items())


#
# ??
# SELECT day, AVG(tip)
# FROM tips
# GROUP BY day;
#


#
# ORQUESTADOR:
#
def run():
    """Orquestador"""

    run_mapreduce_job(
        mapper_fn=mapper_query_1,
        reducer_fn=reducer_query_1,
        input_folder="files/input/",
        output_folder="files/query_1/",
    )

    run_mapreduce_job(
        mapper_fn=mapper_query_2,
        reducer_fn=reducer_query_2,
        input_folder="files/input/",
        output_folder="files/query_2/",
    )

    run_mapreduce_job(
        mapper_fn=mapper_query_3,
        reducer_fn=reducer_query_3,
        input_folder="files/input/",
        output_folder="files/query_3/",
    )

    run_mapreduce_job(
        mapper_fn=mapper_query_4,
        reducer_fn=reducer_query_4,
        input_folder="files/input/",
        output_folder="files/query_4/",
    )

    run_mapreduce_job(
        mapper_fn=mapper_query_5,
        reducer_fn=reducer_query_5,
        input_folder="files/input/",
        output_folder="files/query_5/",
    )


if _name_ == "_main_":

    run()
    

