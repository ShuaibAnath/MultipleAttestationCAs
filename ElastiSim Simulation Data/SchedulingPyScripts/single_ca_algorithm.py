from typing import Any
from elastisim_python import JobState, JobType, NodeState, pass_algorithm, Job, Node, InvocationType
import pandas as pd
 
 
# TODO: Add function to assign fog node to beginning of assigned nodes list
# TODO: If the above does not work, just make the issuance a delay task. Or begin with it as a delay task initially.
# TODO: Generate more realistic bursty submit times so that turnaround time graph is not smooth.
QUEUE_LENGTH = 5
NUMBER_OF_CA_NODES = 1
WAIT_TIME_THRESHOLD = 0.03

# unavailability_start_time = 0.0
# unavailability_total_time = 0.0
# unavailability_timer_started = False

arrival_rate = 30

queue_data_df = pd.DataFrame(columns=['Time', 'queue_length','load','Running Jobs','Invocation Type', 'Allocated CA Nodes','Job ID'])
#  queue_data_df = pd.DataFrame(columns=['Submit Time', 'queue_length','load','Running Jobs','Invocation Type', 'Allocated CA Nodes','Job ID']) # Previous version
#  load_data_df = pd.DataFrame(columns=['End Time', 'queue_length','load','Running Jobs','Invocation Type', 'Allocated CA' ,'Job ID']) # Previous version
#  unavailability_data_single_df = pd.DataFrame(columns=['Start Time','Current Time','Total Unavailabililty Time','queue_length','Allocated CA Nodes','Invocation Type'])

def schedule(jobs: list[Job], nodes: list[Node], system: dict[str, Any]) -> None: 
    print("NEW CALL...")
    time = system['time']
    invocation_type = system['invocation_type']

    print(f"Invocation Type is : {invocation_type}")
    print(f"TIME = {time}")
    print(f"Pending jobs = {[job.identifier for job in jobs if job.state == JobState.PENDING]}")
    print(f"Running jobs = {[job.identifier for job in jobs if job.state == JobState.RUNNING]}")
    print(f"Killed jobs = {[job.identifier for job in jobs if job.state == JobState.KILLED]}")
    print(f"Completed jobs = {[job.identifier for job in jobs if job.state == JobState.COMPLETED]}")
    print(f"Allocated Nodes = {[node.identifier for node in nodes if node.state == NodeState.ALLOCATED]}")

    global queue_data_df
    # global load_data_df

    global QUEUE_LENGTH
    global NUMBER_OF_CA_NODES
    global WAIT_TIME_THRESHOLD

    # global unavailability_start_time
    # global unavailability_total_time
    # global unavailability_data_double_df
    # global unavailability_timer_started

    pending_jobs = [job for job in jobs if job.state == JobState.PENDING]
    running_jobs = [job for job in jobs if job.state == JobState.RUNNING]
    allocated_nodes = [node.identifier for node in nodes if node.state == NodeState.ALLOCATED and node.identifier < 1]

    # Capturing queue data for analysis 
    job_on_invocation = system['job']
    queue_data_df.loc[len(queue_data_df)] = [time, len(pending_jobs), len(pending_jobs) + len(running_jobs),len(running_jobs),
                                             invocation_type, allocated_nodes,f'job{job_on_invocation.identifier}']

     # If none of the CA nodes are free and  queue is full start unavailability timer
    # if len(pending_jobs) >= QUEUE_LENGTH and nodes[0].state == NodeState.ALLOCATED :
    #     if not unavailability_timer_started:
    #         # print("Unavalability Timer Started")
    #         unavailability_start_time = time
    #         if invocation_type == InvocationType.INVOKE_JOB_SUBMIT:
    #             unavailability_start_time = round(unavailability_start_time,5) 
    #         unavailability_timer_started = True
    # elif  len(pending_jobs) < QUEUE_LENGTH or nodes[0].state == NodeState.FREE:
    #     if unavailability_timer_started:
    #         unavailability_total_time = unavailability_total_time + time - unavailability_start_time
    #     unavailability_timer_started = False
    #     unavailability_data_single_df.loc[len(unavailability_data_single_df)] = [unavailability_start_time, time, unavailability_total_time, len(pending_jobs), allocated_nodes,invocation_type]

    # Capturing queue data for analysis (not part of the algorithm)
    # if invocation_type == InvocationType.INVOKE_JOB_SUBMIT or invocation_type == InvocationType.INVOKE_JOB_COMPLETED:
    #     queue_data_df.loc[len(queue_data_df)] = [time, len(pending_jobs), len(pending_jobs) + len(running_jobs)]
    # if invocation_type == InvocationType.INVOKE_JOB_SUBMIT:
    #     job_on_invocation = system['job']
    #     queue_data_df.loc[len(queue_data_df)] = [time, len(pending_jobs), len(pending_jobs) + len(running_jobs),len(running_jobs),
    #                                               invocation_type, allocated_nodes,f'job{job_on_invocation.identifier}']
    
    # if  invocation_type == InvocationType.INVOKE_JOB_COMPLETED:
    #     job_on_invocation = system['job']
    #     load_data_df.loc[len(load_data_df)] = [time, len(pending_jobs), len(pending_jobs) + len(running_jobs),len(running_jobs),
    #                                               invocation_type, allocated_nodes,f'job{job_on_invocation.identifier}']
    

    # Check if queue has passed capacity(or overflowed) on job submission
    if invocation_type == InvocationType.INVOKE_JOB_SUBMIT and len(pending_jobs) > QUEUE_LENGTH:
            print(f"Killing Job {pending_jobs[-1].identifier}")
            pending_jobs[-1].kill()

    # Else if queue length not exceeded, and the invocation is on a job submission or completion
    elif system['invocation_type'] == InvocationType.INVOKE_JOB_SUBMIT or invocation_type == InvocationType.INVOKE_JOB_COMPLETED:
        current_job = system['job']
        # then check if CA node is free. 
        if nodes[0].state == NodeState.FREE:
            print("CA NODE IS FREE")
            for job in jobs:
                # First job found in pending queue is assigned the CA node and a fog node from the nodes list
                if len(job.assigned_node_ids) == 0 and job.state == JobState.PENDING and not job.kill_flag:
                    if (not (0 in job.assigned_node_ids)):
                        fog_node_id = job.identifier + 1
                        print(f"Nodes to assign to job {job.identifier} : {[nodes[0].identifier, nodes[fog_node_id].identifier]}") 
                        job.assign(nodes=[nodes[0], nodes[fog_node_id]])
                        break


    # If the scheduler is invoked at a scheduling point
    elif invocation_type == InvocationType.INVOKE_SCHEDULING_POINT:
        current_job = system['job']
        print(f"Number of completed phases for scheduling point: {current_job.completed_phases}")
        # Retrieve fog node based on Job ID. Fog node ID = Job ID + Number_of_CAs
        fog_node_id = current_job.identifier + 1
  
        # if current_job.wait_time > WAIT_TIME_THRESHOLD:
        #     print("Wait time exceeded")
        #     current_job.kill()
        #     print(f"Kill flag = {current_job.kill_flag} for job {current_job.identifier}")
        #     # If the job is going to be killed, start the next job 
        #     if current_job.kill_flag:
        #         nodes_to_assign = [nodes[fog_node_id+1],nodes[0]] # Assign CA node
        #         print(f"Nodes to assign {[node.identifier for node in nodes_to_assign]}")
        #         #Check if pending jobs is empty
        #         if pending_jobs:
        #             pending_jobs[0].assign(nodes=nodes_to_assign)
        #             print(f"Pending job {pending_jobs[0].identifier} assigned nodes {[node.identifier for node in nodes_to_assign]}")

        
        # Check if the Certificate Request task is complete
        if current_job.completed_phases < 2 and len(current_job.assigned_node_ids) == (NUMBER_OF_CA_NODES + 1) :
            print(f"Removing fog node {fog_node_id}") 
            current_job.remove(nodes[fog_node_id]) # Remove fog node so that the computation in the next phase is done only by the CA nodes

        else:
            nodes_to_assign = [nodes[fog_node_id],nodes[0]] # Assign CA node
            current_job.remove(current_job.assigned_nodes)
            print(f"Nodes to assign {[node.identifier for node in nodes_to_assign]}")
            current_job.assign(nodes=nodes_to_assign)
            print(f" Current assigned jobs for job {current_job.identifier} is {[node.identifier for node in current_job.assigned_nodes]}")
    
    

if __name__ == '__main__':
    url = 'ipc:///tmp/elastisim.ipc'
    pass_algorithm(schedule, url)
    queue_data_df.to_csv(f'queue_data_single_arrival_rate_{arrival_rate}.csv', index=False)
    # load_data_df.to_csv('load_data_single.csv', index=False)
    # unavailability_data_single_df.to_csv('unavailability_data_single.csv', index=False)
