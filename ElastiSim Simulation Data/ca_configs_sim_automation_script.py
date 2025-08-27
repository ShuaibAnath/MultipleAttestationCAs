import subprocess
import os
import json
import shutil
from lxml import etree

NUMBER_OF_CAS = 3

def get_jobs_array_size(file_path):
    """
    Reads a JSON file and returns the size of the array associated with the key "jobs".

    Args:
    file_path (str): Path to the JSON file.

    Returns:
    int: Size of the "jobs" array.
    """
    try:
        # Open the JSON file and load its content
        with open(file_path, 'r') as file:
            data = json.load(file)
        
        # Check if the "jobs" key exists and is a list
        if "jobs" in data and isinstance(data["jobs"], list):
            return len(data["jobs"])
        else:
            raise ValueError('The key "jobs" is not present or is not an array in the JSON file.')
    except FileNotFoundError:
        print(f"The file {file_path} does not exist.")
    except json.JSONDecodeError:
        print("Error decoding JSON file.")
    except ValueError as ve:
        print(ve)


def update_radical_in_xml(xml_file_path, jobs_count):
    """
    Updates the radical attribute in the XML file based on the jobs count.

    Args:
    xml_file_path (str): Path to the XML file.
    jobs_count (int): Number of jobs to set in the radical attribute.
    """
    try:
        # Parse the XML file
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(xml_file_path, parser)
        root = tree.getroot()
        
        # Find the cluster element and update the radical attribute
        cluster = root.xpath(".//cluster[@id='Crossbar']")
        if cluster:
            cluster = cluster[0]
            new_radical_value = f"100-{100 + NUMBER_OF_CAS + jobs_count - 1}"
            cluster.set("radical", new_radical_value)
            
            # Write the updated XML back to file, preserving the XML declaration and DOCTYPE
            tree.write(xml_file_path, pretty_print=True, xml_declaration=True, doctype='<!DOCTYPE platform SYSTEM "https://simgrid.org/simgrid.dtd">', encoding='UTF-8')
            print(f"Updated radical attribute to {new_radical_value} in {xml_file_path}")
        else:
            print("No cluster element with id 'Crossbar' found in the XML file.")
    except etree.XMLSyntaxError:
        print("Error parsing the XML file.")
    except FileNotFoundError:
        print(f"The file {xml_file_path} does not exist.")

def execute_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    return output, error

def copy_and_rename_files(src_dir, dest_dir, file1, file2, new_name1, new_name2):
    """
    Copies two files from a source directory to a destination directory and renames them.
    :param src_dir: Source directory path
    :param dest_dir: Destination directory path
    :param file1: Name of the first file to copy
    :param file2: Name of the second file to copy
    :param new_name1: New name for the first file in the destination directory
    :param new_name2: New name for the second file in the destination directory
    """
    
    # Construct full file paths
    src_file1 = os.path.join(src_dir, file1)
    src_file2 = os.path.join(src_dir, file2)
    
    dest_file1 = os.path.join(dest_dir, new_name1)
    dest_file2 = os.path.join(dest_dir, new_name2)
    
    # Copy and rename the files
    shutil.copy(src_file1, dest_file1)
    shutil.copy(src_file2, dest_file2)


def copy_paste_and_rename_file(src_dir, dest_dir, file, new_name):
    """
    Copies two files from a source directory to a destination directory and renames them.
    :param src_dir: Source directory path
    :param dest_dir: Destination directory path
    :param file: Name of the first file to copy
    :param new_name: New name for the first file in the destination directory
    """
    
    # Construct full file paths
    src_file = os.path.join(src_dir, file)
    dest_file = os.path.join(dest_dir, new_name)
    
    # Copy and rename the files
    shutil.copy(src_file, dest_file)


def copy_text_to_json(text_file_path, json_file_path):
  """
  Copies all text from a text file and writes it into a JSON file.

  Args:
    text_file_path: Path to the text file containing valid JSON data.
    json_file_path: Path to the output JSON file.
  """

  with open(text_file_path, 'r') as text_file:
    json_data = text_file.read()

  # Assuming the text file content is already valid JSON, directly write it to the JSON file
  with open(json_file_path, 'w') as json_file:
    json.dump(json.loads(json_data), json_file, indent=4)  # Add indentation for readability


def run_simulation(arrival_rate):
  """
  Runs the simulation for a given arrival rate, configuration, script, and saves results.

  Args:
    arrival_rate: The arrival rate used for data generation.
    config_file: Path to the configuration file.
    algo_script: Path to the scheduling algorithm script.
    result_dir: Directory to store the simulation results.
  """
  # Generate input data
  subprocess.run(["python3", "job_arrival_times_generator.py", "--arrival_rate", str(arrival_rate)])
  ca_configs = ['single_ca','double_ca','triple_ca']
  configs = ['single','double','triple']

  config_files = ["single_ca_configuration.json",
                  "double_ca_configuration.json",  
                  "triple_ca_configuration.json" ]

  algo_scripts = ["single_ca_algorithm.py",
                  "double_ca_algorithm.py",
                  "triple_ca_algorithm.py"]

  # Update single ca JSON job files
  copy_text_to_json("single_ca_jobs_arrival_rate.txt", "data/input/single_ca_jobs.json")
  # Update XML file for appropriate number of nodes
  # Only require a change to the crossbar.xml using either the single_ca_jobs.json or multi_ca_jobs.json
  json_file_path = 'data/input/single_ca_jobs.json'
  xml_file_path = 'data/input/crossbar.xml'
  # Get the number of jobs from the JSON file
  jobs_count = get_jobs_array_size(json_file_path)
  # Update the radical attribute in the XML file
  if jobs_count is not None:
    update_radical_in_xml(xml_file_path, jobs_count)

  # Process the single_ca part
  command = f'./elastisim data/input/{config_files[0]} | python3 {algo_scripts[0]}'
  
  output, error = execute_command(command)
  # Check if there was any error
  if error:
      print("An error occurred:", error.decode())
  else:
      print("Command executed successfully")
  src_dir = 'data/output'
  dest_dir = 'data/arrival_rate_stats/single_ca_job_stats'
  src_file_1 = 'single_ca_job_statistics.csv'
  src_file_2 = 'queue_data_single.csv'
  new_file_1 = f'single_ca_job_statistics_arrival_rate_{arrival_rate}.csv'
  new_file_2 = f'queue_data_single_arrival_rate_{arrival_rate}.csv'
  copy_and_rename_files(src_dir, dest_dir, src_file_1, src_file_2, new_file_1, new_file_2)

  dest_directory = f'data/arrival_rate_stats/single_ca_task_times'
  src_file = f'single_ca_task_times.csv'
  new_file= f'single_ca_task_times_arrival_rate_{arrival_rate}.csv'
  copy_paste_and_rename_file(src_dir, dest_directory, src_file, new_file)

  # Update multi ca JSON job files
  copy_text_to_json("multi_ca_jobs_arrival_rate.txt", "data/input/multi_ca_jobs.json")
  # Process the multi_ca parts
  for config_file, algo_script in zip(config_files[1:], algo_scripts[1:]) :
      command = f'./elastisim data/input/{config_file} | python3 {algo_script}'
      output, error = execute_command(command)
      
      # Check if there was any error
      if error:
          print("An error occurred:", error.decode())
      else:
          print("Command executed successfully")

      for i in range(len(ca_configs) - 1):
        src_dir = 'data/output'
        dest_dir = f'data/arrival_rate_stats/{ca_configs[i+1]}_job_stats'
        src_file_1 = f'{ca_configs[i+1]}_job_statistics.csv'
        src_file_2 = f'queue_data_{configs[i+1]}.csv'
        new_file_1 = f'{ca_configs[i+1]}_job_statistics_arrival_rate_{arrival_rate}.csv'
        new_file_2 = f'queue_data_{configs[i+1]}_arrival_rate_{arrival_rate}.csv'
        copy_and_rename_files(src_dir, dest_dir, src_file_1, src_file_2, new_file_1, new_file_2)

        dest_directory = f'data/arrival_rate_stats/{ca_configs[i+1]}_task_times'
        src_file = f'{ca_configs[i+1]}_task_times.csv'
        new_file= f'{ca_configs[i+1]}_task_times_arrival_rate_{arrival_rate}.csv'
        copy_paste_and_rename_file(src_dir, dest_directory, src_file, new_file)


def main():
#   for arrival_rate in range(505, 1001, 5):
#     run_simulation(arrival_rate)

  for arrival_rate in range(150, 201, 5):
    run_simulation(arrival_rate)

if __name__ == "__main__":
  main()

