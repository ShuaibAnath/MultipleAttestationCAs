from typing import Any
from elastisim_python import JobState, JobType, NodeState, pass_algorithm, Job, Node, InvocationType
import pandas as pd

QUEUE_LENGTH = 5
NUMBER_OF_CA_NODES = 3
WAIT_TIME_THRESHOLD = 0.03

arrival_rate = 30

# unavailability_start_time = 0.0
# unavailability_total_time = 0.0
# unavailability_timer_started = False

jobs_assigned_cas = {}
queue_data_triple_df = pd.DataFrame(columns=['Time', 'queue_length','load','Running Jobs','Invocation Type', 'Allocated CA Nodes','Job ID'])

# queue_data_triple_df = pd.DataFrame(columns=['Submit Time', 'queue_length', 'load','Job ID'])
# load_data_triple_df = pd.DataFrame(columns=['End Time', 'queue_length','load','Job ID'])
# unavailability_data_triple_df = pd.DataFrame(columns=['Start Time','Current Time','Total Unavailabililty Time','queue_length','Allocated CA Nodes','Invocation Type'])

# TODO: Add a delay task for blockchain setup etc. Or increase the time for the Certificate generation task. REVIEW this.
# TODO: Change the certificate request task to broadcast or to a single random node? Which is better?
# TODO: Pick a random number from 0 to NUMBER_OF_CA_NODES. Then assign the fog node and the random
      # CA node to the certificate issuance task so that only one CA node issues the certificate
      # TODO: Ensure that the minimum number of nodes is changed to 2 from 3 in the multijobs.json file.
# TODO: How to implement overflow or colaborative strategy for certificate generation?
# TODO: How to impelement a combination of both strategies with overflow taking priority?
# NOTE: The simulator deallocates the nodes on its own on Job completion

def schedule(jobs: list[Job], nodes: list[Node], system: dict[str, Any]) -> None: 
    time = system['time']
    global QUEUE_LENGTH
    global NUMBER_OF_CA_NODES
    global WAIT_TIME_THRESHOLD

    global jobs_assigned_cas
    global queue_data_triple_df
    # global load_data_triple_df

    # global unavailability_start_time
    # global unavailability_total_time
    # global unavailability_data_single_df
    # global unavailability_timer_started

    pending_jobs = [job for job in jobs if job.state == JobState.PENDING]
    running_jobs = [job for job in jobs if job.state == JobState.RUNNING]
    allocated_nodes = [node.identifier for node in nodes if node.state == NodeState.ALLOCATED and node.identifier < 3]
    invocation_type = system['invocation_type']

    print("NEW CALL...")
    print(f"TIME = {time}")
    print(f"Pending jobs = {[job.identifier for job in jobs if job.state == JobState.PENDING]}")
    print(f"Running jobs = {[job.identifier for job in jobs if job.state == JobState.RUNNING]}")
    print(f"Killed jobs = {[job.identifier for job in jobs if job.state == JobState.KILLED]}")
    print(f"Completed jobs = {[job.identifier for job in jobs if job.state == JobState.COMPLETED]}")
    print(f"Allocated Nodes = {[node.identifier for node in nodes if node.state == NodeState.ALLOCATED]}")
    print(f"Invocation is : {invocation_type}")

    job_on_invocation = system['job']
    queue_data_triple_df.loc[len(queue_data_triple_df)] = [time, len(pending_jobs), len(pending_jobs) + len(running_jobs),len(running_jobs),
                                                            invocation_type, allocated_nodes,f'job{job_on_invocation.identifier}']
    

    # If none of the CA nodes are free and  queue is full start unavailability timer
    # if len(pending_jobs) >= QUEUE_LENGTH and nodes[0].state == NodeState.ALLOCATED and nodes[1].state == NodeState.ALLOCATED and nodes[2].state == NodeState.ALLOCATED:
    #     if not unavailability_timer_started:
    #         # print("Unavalability Timer Started")
    #         unavailability_start_time = time
    #         if invocation_type == InvocationType.INVOKE_JOB_SUBMIT:
    #             unavailability_start_time = round(unavailability_start_time,5) 
    #         unavailability_timer_started = True
    # elif  len(pending_jobs) < QUEUE_LENGTH or nodes[0].state == NodeState.FREE or nodes[1].state == NodeState.FREE or nodes[2].state == NodeState.FREE:
    #     if unavailability_timer_started:
    #         unavailability_total_time = unavailability_total_time + time - unavailability_start_time
    #     unavailability_timer_started = False
    #     unavailability_data_triple_df.loc[len(unavailability_data_triple_df)] = [unavailability_start_time, time, unavailability_total_time, len(pending_jobs), allocated_nodes,invocation_type]


    # Capturing queue data, submit and complete times
    # if invocation_type == InvocationType.INVOKE_JOB_SUBMIT or invocation_type == InvocationType.INVOKE_JOB_COMPLETED:
    #     queue_data_triple_df.loc[len(queue_data_triple_df)] = [time, len(pending_jobs), len(pending_jobs) + len(running_jobs)]

    # if invocation_type == InvocationType.INVOKE_JOB_SUBMIT:
    #     job_on_invocation = system['job']
    #     queue_data_triple_df.loc[len(queue_data_triple_df)] = [time, len(pending_jobs), len(pending_jobs) + len(running_jobs),f'job{job_on_invocation.identifier}']
    
    # if  invocation_type == InvocationType.INVOKE_JOB_COMPLETED:
    #     job_on_invocation = system['job']
    #     load_data_triple_df.loc[len(load_data_triple_df)] = [time, len(pending_jobs), len(pending_jobs) + len(running_jobs), f'job{job_on_invocation.identifier}']

    # Check if queue has passed capacity(or overflowed) on job submission
    if invocation_type == InvocationType.INVOKE_JOB_SUBMIT and len(pending_jobs) > QUEUE_LENGTH:
        print(f'Q legnth = {len(pending_jobs)}')
        print(f"Killing Job {pending_jobs[-1].identifier}")
        pending_jobs[-1].kill()
    
    elif invocation_type == InvocationType.INVOKE_JOB_SUBMIT or invocation_type == InvocationType.INVOKE_JOB_COMPLETED:
    # Else if the queue length is not exceeded or a job has completed
        # Check if the CA nodes are allocated
        if nodes[0].state == NodeState.FREE:
            for pending_job in pending_jobs:
                # First job found in pending queue is set to running state
                if len(pending_job.assigned_node_ids) == 0 and not pending_job.kill_flag:
                    if (not (0 in pending_job.assigned_node_ids)):
                        fog_node_id = pending_job.identifier + NUMBER_OF_CA_NODES
                        nodes_to_assign = [nodes[0]] # Assign CA node 0 based on pending jobs
                        nodes_to_assign.insert(0,nodes[fog_node_id]) # Assign Fog node to beginning
                        print(f"CA Node to assign to job {pending_job.identifier} : {[nodes[0].identifier]}")
                        pending_job.assign(nodes=nodes_to_assign)
                        print(f" Current assigned nodes for job {pending_job.identifier} is {[node.identifier for node in pending_job.assigned_nodes]}")
                        break # Have to break because the CA nodes must be exclusively assigned to the first job found
        
        elif nodes[1].state == NodeState.FREE:
            # First job found in pending queue is assigned CA node 1 and a fog node
            for pending_job in pending_jobs:
                if len(pending_job.assigned_node_ids) == 0 and not pending_job.kill_flag:
                    if (not (1 in pending_job.assigned_node_ids)):
                        fog_node_id = pending_jobs[0].identifier + NUMBER_OF_CA_NODES
                        nodes_to_assign = [nodes[1]] # Assign CA node 1 based on pending jobs
                        nodes_to_assign.insert(0,nodes[fog_node_id]) # Assign Fog node to beginning
                        print(f" CA Node to assign to job {pending_job.identifier} : {[nodes[1].identifier]}")
                        pending_job.assign(nodes=nodes_to_assign)
                        print(f" Current assigned nodes for job {pending_job.identifier} is {[node.identifier for node in pending_job.assigned_nodes]}")
                        break # Have to break because the CA nodes must be exclusively assigned to the first job found

        elif nodes[2].state == NodeState.FREE:
            # First job found in pending queue is assigned CA node 2 and a fog node
            for pending_job in pending_jobs:
                if (not (2 in pending_job.assigned_node_ids)):
                    fog_node_id = pending_job.identifier + NUMBER_OF_CA_NODES
                    nodes_to_assign = [nodes[2]] # Assign CA node 2 based on pending jobs
                    nodes_to_assign.insert(0,nodes[fog_node_id]) # Assign Fog node to beginning
                    print(f" CA Node to assign to job {pending_job.identifier} : {[nodes[2].identifier]}")
                    pending_job.assign(nodes=nodes_to_assign)
                    print(f" Current assigned nodes for job {pending_job.identifier} is {[node.identifier for node in pending_job.assigned_nodes]}")
                    break # Have to break because the CA nodes must be exclusively assigned to the first job found

    
    elif invocation_type == InvocationType.INVOKE_SCHEDULING_POINT:
    # Else if the certificate request task or the certificate generation task has completed
    # Remove fog node for certificate generation task or insert fog node for certificate issuance task, respectively.
        current_job = system['job']
        print(f"Number of completed phases for job {current_job.identifier} at scheduling point: {current_job.completed_phases}")
        fog_node_id = current_job.identifier + NUMBER_OF_CA_NODES # Retrieve fog node based on Job ID. Fog node ID = Job ID + Number_of_CAs

        # Check if job wait time has exceeded the threshold
        # if current_job.wait_time > WAIT_TIME_THRESHOLD:
        #     print("Wait time exceeded")
        #     # Retrieve fog node based on Job ID. Fog node ID = Job ID + Number_of_CAs
        #     fog_node_id = current_job.identifier + NUMBER_OF_CA_NODES
        #     print(f"Nodes assigned to job {current_job.identifier} is {[node_id for node_id in list(current_job.assigned_node_ids)]}")
        #     assigned_ca_node_id = min(list(current_job.assigned_node_ids)) # Get assigned CA node
        #     nodes_to_assign = [nodes[assigned_ca_node_id]] # Assign CA node to next job after current job is killed
        #     current_job.kill()
        #     print(f"Kill flag = {current_job.kill_flag} for job {current_job.identifier}")
            
            # If the job is going to be killed, start the next job 
            # if current_job.kill_flag:
            #     print(f"Fog node ID = {fog_node_id}")
            #     while not (nodes[fog_node_id].state == NodeState.FREE):
            #     #Iterate through the nodes until the first node found is free
            #         fog_node_id+=1
                
            #     nodes_to_assign.insert(0,nodes[fog_node_id]) # Assign succesive fog node to beginning
            #     print(f"Nodes to assign after KILL {[node.identifier for node in nodes_to_assign]}")
            #     #Check if pending jobs is empty
            #     if pending_jobs:
            #         pending_jobs[0].assign(nodes=nodes_to_assign)
            #         print(f"Pending job {pending_jobs[0].identifier} assigned nodes {[node.identifier for node in nodes_to_assign]}")       
 
        
        if current_job.completed_phases < 2 and len(current_job.assigned_node_ids) == 2:
        # If the scheduler is invoked at a scheduling point, in this case after Certificate Request task
        # Then remove fog node from assigned nodes because the request communication task has completed
            print(f"Nodes assigned to job {current_job.identifier} is {[node_id for node_id in list(current_job.assigned_node_ids)]}")
            assigned_ca_node_id = min(list(current_job.assigned_node_ids)) # Get assigned CA node
            jobs_assigned_cas[str(current_job.identifier)] = assigned_ca_node_id # Store Assigned CA node
            print(f"Removing fog node {fog_node_id}") 
            current_job.remove(nodes[fog_node_id]) # Remove fog node so that the computation in the next phase is done only by the CA nodes
        else:
        # If it is not the request communication task, then it is the issuance communication task
            ca_node_issuer = jobs_assigned_cas[str(current_job.identifier)] # Retrieve CA node ID using job ID as a key
            nodes_to_assign = [nodes[ca_node_issuer]] # Assign CA node to issue certificate
            nodes_to_assign.insert(0,nodes[fog_node_id]) # Assign Fog node to beginning
            current_job.remove(current_job.assigned_nodes) # Remove all assigned nodes to reorder assignment for certificate issuance
            print(f"Nodes to assign {[node.identifier for node in nodes_to_assign]}")
            current_job.assign(nodes=nodes_to_assign)
            print(f" Current assigned ndoes for job {current_job.identifier} is {[node.identifier for node in current_job.assigned_nodes]}")

if __name__ == '__main__':
    url = 'ipc:///tmp/elastisim.ipc'
    pass_algorithm(schedule, url)
    queue_data_triple_df.to_csv(f'queue_data_triple_arrival_{arrival_rate}.csv', index=False)
    # load_data_triple_df.to_csv('load_data_triple.csv', index=False)
    # unavailability_data_triple_df.to_csv('unavailability_data_triple.csv', index=False)
