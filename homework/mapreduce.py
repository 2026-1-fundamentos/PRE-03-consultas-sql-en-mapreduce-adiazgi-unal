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
    

