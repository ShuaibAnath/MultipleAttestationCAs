import numpy as np
import plotly.graph_objects as go
import random
import argparse
from statistics import mean


def generate_poisson_process(rate, time_horizon):
  # Generate inter-arrival times
  inter_arrival_times = np.random.exponential(1/rate, size=int(rate*time_horizon*1.5))
  # Compute arrival times
  arrival_times = np.cumsum(inter_arrival_times)
  # Filter arrival times within the time horizon
  arrival_times = arrival_times[arrival_times < time_horizon]
  # Round off arrival times to 5 decimal places and convert to string to avoid scientific notation
  arrival_times = np.round(arrival_times, 5)
  arrival_times = [f"{x:.5f}" for x in arrival_times]
  # Convert arrival times back to float for plotting
  arrival_times = [float(time) for time in arrival_times]
  return arrival_times

def simulate_poisson_process(lambda_rate, num_jobs):
    """
    Simulate the arrival times of jobs using a Poisson process.
    
    Parameters:
    - lambda_rate (float): The average rate (jobs per unit time).
    - num_jobs (int): The number of job arrivals to simulate.
    
    Returns:
    - List of arrival times (floats).
    """
    # Generate interarrival times from an exponential distribution
    interarrival_times = generate_inter_arrival_times(target_lambda=lambda_rate,num_points=num_jobs)
    # interarrival_times = np.random.exponential(1/lambda_rate, num_jobs)
    # Compute arrival times by taking the cumulative sum of interarrival times
    arrival_times = np.cumsum(interarrival_times)
    
    return list(arrival_times)

def generate_inter_arrival_times(target_lambda, num_points, tolerance=2):
  """
  Generates a list of arrival times with an average arrival rate close to the target lambda.

  Args:
      target_lambda: The desired average arrival rate (lambda value).
      tolerance: The allowed tolerance for the calculated lambda (default: 2).

  Returns:
      A list of arrival times and the calculated lambda value, or None if unable to find
      a list within the tolerance after a certain number of attempts.
  """
  max_attempts = 1000  # Set a maximum number of attempts to avoid infinite loop

  for _ in range(max_attempts):
    inter_arrival_times = np.random.exponential(scale=1/target_lambda, size=num_points)
    calculated_lambda = 1 / mean(inter_arrival_times)

    # Check if calculated lambda is within tolerance of target lambda
    if abs(calculated_lambda - target_lambda) <= tolerance:
      return inter_arrival_times.tolist()

  # Maximum attempts reached, unable to find a suitable list
  return None


def generate_submission_times_within_range(avg_arrival_rate, num_jobs, start_time, end_time):
  """
  Generate a list of job submission times following an exponential distribution within a specified range.

  Parameters:
  avg_arrival_rate (float): The average number of arrivals per unit time.
  num_jobs (int): The number of job submission times to generate.
  start_time (float): The start time of the range.
  end_time (float): The end time of the range.

  Returns:
  list: A list of job submission times within the specified range.
  """
  if start_time >= end_time:
      raise ValueError("start_time must be less than end_time")
  
  # Lambda rate is the average arrival rate
  target_lambda = avg_arrival_rate
  submission_times = []
  
  while len(submission_times) < num_jobs:
      # Generate inter-arrival times
      inter_arrival_times = generate_inter_arrival_times(target_lambda,num_points=100, tolerance=2)

      # Generate cumulative submission times starting from start_time
      candidate_times = start_time + np.cumsum(inter_arrival_times)
      
      # Filter times to be within the specified range
      candidate_times = candidate_times[candidate_times <= end_time]
      
      # Add valid times to the submission_times list
      submission_times.extend(candidate_times)
  
  # If we generated too many times, trim the list to the required number of jobs
  submission_times = submission_times[:num_jobs]
  print(submission_times)
  return submission_times


def generate_single_ca_jobs(arrival_times):
  """
  This function generates a text file containing the input data for jobs of the single CA experiment

  Args:
      arrival_times (list:float): List of arrival times to assign to the submit times of jobs
  Returns:
      None
  """
  f = open("single_ca_jobs_arrival_rate.txt","w+")
  f.write("{\n   \"jobs\": [\n    \n")

  for index, arrival_time in enumerate(arrival_times):
    if index == len(arrival_times) - 1:
      f.write("  {\n    \"type\": \"malleable\",\n    \"submit_time\": %.8f,\n     \"num_nodes_min\": 1,\n    \"num_nodes_max\": 2,\n    \"application_model\": \"data/input/single_ca_application_model.json\",\n    \"arguments\": {\n      \"flops\": 25520000,\n      \"iterations\": 1,\n      \"request_communication_size\": 4728,\n     \"issuance_communication_size\": 2108\n   }\n }\n" %arrival_time)
    else:
      f.write("  {\n    \"type\": \"malleable\",\n    \"submit_time\": %.8f,\n     \"num_nodes_min\": 1,\n    \"num_nodes_max\": 2,\n    \"application_model\": \"data/input/single_ca_application_model.json\",\n    \"arguments\": {\n      \"flops\": 25520000,\n      \"iterations\": 1,\n      \"request_communication_size\": 4728,\n     \"issuance_communication_size\": 2108\n   }\n },\n" %arrival_time)

  f.write("    ]\n}")
  f.close()


def generate_multi_ca_jobs(arrival_times):
  """
  This function generates a text file containing the input data for jobs of the multi CA experiment

  Args:
    arrival_times (list:float): List of arrival times to assign to the submit times of jobs
  Returns:
    None
  """
    
  f = open("multi_ca_jobs_arrival_rate.txt","w+")
  f.write("{\n   \"jobs\": [\n    \n")

  for index, arrival_time in enumerate(arrival_times):
    if index == len(arrival_times) - 1:
      f.write("  {\n    \"type\": \"malleable\",\n    \"submit_time\": %.8f,\n     \"num_nodes_min\": 1,\n    \"num_nodes_max\": 4,\n    \"application_model\": \"data/input/multi_ca_application_model.json\",\n    \"arguments\": {\n      \"flops\": 25520000,\n      \"iterations\": 1,\n      \"request_communication_size\": 4728,\n     \"issuance_communication_size\": 2108\n   }\n }\n" %arrival_time)
    else:
      f.write("  {\n    \"type\": \"malleable\",\n    \"submit_time\": %.8f,\n     \"num_nodes_min\": 1,\n    \"num_nodes_max\": 4,\n    \"application_model\": \"data/input/multi_ca_application_model.json\",\n    \"arguments\": {\n      \"flops\": 25520000,\n      \"iterations\": 1,\n      \"request_communication_size\": 4728,\n     \"issuance_communication_size\": 2108\n   }\n },\n" %arrival_time)
  
  f.write("    ]\n}")
  f.close()


def main(arrival_rate):
  print("In jobs_arrival_generator Arrival rate = ", arrival_rate)

  # Parameters
  lambda_rate = arrival_rate  # rate of 5 arrivals per unit time
  num_jobs = 100 # simulate for 5 units of time

  # Generate the Poisson process
  submission_times = simulate_poisson_process(lambda_rate, num_jobs)
  # submission_times = generate_submission_times_within_range(avg_arrival_rate, num_jobs, start_time=0, end_time=1)
  
  submission_times_sorted = np.sort(submission_times)  # Sort submission times and create array index
  generate_single_ca_jobs(submission_times_sorted)
  generate_multi_ca_jobs(submission_times_sorted)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Generate job arrival times")
  parser.add_argument("--arrival_rate", type=float, help="Arrival rate of jobs")
  args = parser.parse_args()
  main(args.arrival_rate)