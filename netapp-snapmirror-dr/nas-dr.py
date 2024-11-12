import yaml
import paramiko
import time
import concurrent.futures
import os
import platform
import pytz
from tzlocal import get_localzone
from datetime import datetime
from getpass import getpass
from tabulate import tabulate
from termcolor import colored
from colorama import init
init()

RECENT_ACTIONS_FILE = r"\\hostname.domain\recent_actions_log.yaml"

# Define valid actions
VALID_ACTIONS = [
    "show details", "update", "quiesce", "break", "resync", 
    "recovery", "recovery-extended", "restoration-extended", 
    "restoration-flip-flop", "restoration-post-tvt"
]


# Function to connect to the cluster using Paramiko for SSH
def ssh_connect(cluster, username, password):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(cluster, username=username, password=password)
    return ssh

# Helper function to extract cluster name without domain and convert to all caps
def get_cluster_name(cluster):
    return cluster.split('.')[0].upper()

def load_recent_actions():
    if os.path.exists(RECENT_ACTIONS_FILE):
        with open(RECENT_ACTIONS_FILE, 'r') as f:
            actions = yaml.safe_load(f) or {}
            
            # Ensure each app has a list of actions, even if only one action exists
            for app_name, action in actions.items():
                if not isinstance(action, list):
                    actions[app_name] = [action]
                    
            return actions
    return {}

def save_recent_action(app_name, action, user, timezone):
    actions = load_recent_actions()
    local_tz = pytz.timezone(timezone)
    timestamp = datetime.now(local_tz).strftime("%A %d-%b-%Y %I:%M:%S %p")
    
    # Ensure each app has an action history list
    if app_name not in actions:
        actions[app_name] = []  # Initialize as a list for multiple actions
    
    # Append the new action with timestamp and user to maintain action history
    actions[app_name].append({
        "action": action,
        "user": user,
        "timestamp": timestamp
    })

    # Write the updated actions back to the YAML file
    with open(RECENT_ACTIONS_FILE, 'w') as f:
        yaml.safe_dump(actions, f)




# Function to fetch SnapMirror details using SSH (Paramiko)
def fetch_snapmirror_details(ssh, vserver, volume):
    try:
        command = f"snapmirror show -destination-path {vserver}:{volume} -fields lag-time,state,status,schedule,policy"
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.readlines()
        error = stderr.read()

        if error:
            print(colored(f"Error executing snapmirror show: {error}", "red"))
            return {"lag-time": "Error", "state": "Error", "status": "Error", "schedule": "Error", "policy": "Error"}

        for line in output:
            if vserver in line and volume in line:
                fields = line.split()
                lag_time = fields[-1]
                status = fields[-2]
                state = fields[-3]
                policy = fields[-4]
                schedule = fields[-5]
                return {
                    "lag-time": lag_time,
                    "state": state,
                    "status": status,
                    "schedule": schedule,
                    "policy": policy
                }
        return {"lag-time": "N/A", "state": "N/A", "status": "N/A", "schedule": "N/A", "policy": "N/A"}
    except Exception as e:
        print(colored(f"Failed to execute command: {str(e)}", "red"))
        return {"lag-time": "Error", "state": "Error", "status": "Error", "schedule": "Error", "policy": "Error"}

# Function to fetch DR-to-PROD SnapMirror details
def fetch_dr_to_prod_snapmirror_details(ssh, prod_vserver, volume_name):
    try:
        command = f"snapmirror show -destination-path {prod_vserver}:{volume_name} -fields lag-time,state,status,schedule,policy"
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.readlines()
        error = stderr.read()

        if error:
            print(colored(f"Error executing DR-to-PROD snapmirror show: {error}", "red"))
            return None

        for line in output:
            if "There are no entries matching your query." in line:
                return None  # Skip if no active replication exists
            elif prod_vserver in line and volume_name in line:
                fields = line.split()
                lag_time = fields[-1]
                status = fields[-2]
                state = fields[-3]
                policy = fields[-4]
                schedule = fields[-5]
                return {
                    "lag-time": lag_time,
                    "state": state,
                    "status": status,
                    "schedule": schedule,
                    "policy": policy
                }
        return None
    except Exception as e:
        print(colored(f"Failed to execute DR-to-PROD command: {str(e)}", "red"))
        return None

# Function to perform SnapMirror actions using SSH (Paramiko)
def perform_snapmirror_action(ssh, vserver, volume, action, cluster_name):
    if action == "quiesce":
        command = f"snapmirror quiesce -destination-path {vserver}:{volume}"
    elif action == "break":
        command = f"snapmirror break -destination-path {vserver}:{volume}"
    elif action == "resync":
        command = f"snapmirror resync -destination-path {vserver}:{volume}"
    elif action == "update":
        command = f"snapmirror update -destination-path {vserver}:{volume}"
    else:
        print(colored(f"Invalid action '{action}' specified.", "red"))
        return

    try:
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode("utf-8").strip()
        error = stderr.read().decode("utf-8").strip()

        # Check for the specific warning in output
        if "Warning: All data newer than Snapshot copy" in output:
            print(colored(f"Warning detected for volume '{volume}'. Automatically confirming to proceed...", "yellow"))
            stdin.write("y\n")  # Send 'y' to confirm
            stdin.flush()
            output += stdout.read().decode("utf-8").strip()  # Read remaining output after confirmation

        if error:
            print(colored(f"Error performing '{action}' on {volume}: {error.splitlines()[0]}", "red"))
        else:
            for line in output.splitlines():
                if "Operation succeeded" in line:
                    print(colored(f"{cluster_name}: {line}", "green"))
    except Exception as e:
        print(colored(f"Failed to perform '{action}' on {volume}: {str(e)}", "red"))

# Function to wait for a specific status
def wait_for_status(ssh, vserver, volume, desired_status, cluster_name, message):
    while True:
        details = fetch_snapmirror_details(ssh, vserver, volume)
        if details['status'].lower() == desired_status.lower():
            print(colored(f"{cluster_name}: Volume {volume} is now in '{desired_status}' status.", "green"))
            break
        else:
            print(colored(f"{cluster_name}: {message} for {volume}...", "yellow"))
            time.sleep(5)

# Function to unmount and offline the volume in prod_cluster with detailed error logging
def unmount_and_offline_volume(prod_ssh, prod_vserver, volume, prod_cluster_name):
    # Step 1: Unmount the volume
    unmount_command = f"volume unmount -vserver {prod_vserver} -volume {volume}"
    print(colored(f"{prod_cluster_name}: Running unmount command for volume {volume}...", "cyan"))
    stdin, stdout, stderr = prod_ssh.exec_command(unmount_command)
    output = stdout.read().decode("utf-8").strip()
    error = stderr.read().decode("utf-8").strip()

    if error:
        print(colored(f"Unmount Command Error: {error}", "red"))
    else:
        print(colored(f"{prod_cluster_name}: Volume {volume} successfully unmounted.", "green"))

    # Step 2: Offline the volume
    offline_command = f"volume offline -vserver {prod_vserver} -volume {volume}"
    print(colored(f"{prod_cluster_name}: Running offline command for volume {volume}...", "cyan"))
    stdin, stdout, stderr = prod_ssh.exec_command(offline_command)
    output = stdout.read().decode("utf-8").strip()
    error = stderr.read().decode("utf-8").strip()

    if error:
        print(colored(f"Offline Command Error: {error}", "red"))
    else:
        print(colored(f"{prod_cluster_name}: Volume {volume} successfully offlined in prod cluster", "green"))

# Function to mount the volume in dr_cluster before CIFS share creation
def mount_volume_in_dr(ssh, dr_vserver, volume_name, dr_cluster_name):
    command = f"volume mount -vserver {dr_vserver} -volume {volume_name} -junction-path /{volume_name}"
    print(colored(f"{dr_cluster_name}: Mounting volume '{volume_name}' at junction path '/{volume_name}'...", "cyan"))

    try:
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode("utf-8").strip()
        error = stderr.read().decode("utf-8").strip()

        if error:
            print(colored(f"Error mounting volume '{volume_name}': {error}", "red"))
        elif "Operation succeeded" in output:
            print(colored(f"{dr_cluster_name}: Volume '{volume_name}' successfully mounted.", "green"))

    except Exception as e:
        print(colored(f"Exception occurred while mounting volume '{volume_name}': {str(e)}", "red"))

# Function to create a CIFS share in dr_cluster with detailed logging
def create_cifs_share(ssh, dr_vserver, volume_name, share_name, dr_cluster_name):
    command = f"cifs share create -vserver {dr_vserver} -share-name {share_name} -path /{volume_name}"
    print(colored(f"{dr_cluster_name}: Creating CIFS share '{share_name}' for volume '{volume_name}'...", "cyan"))

    try:
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode("utf-8").strip()
        error = stderr.read().decode("utf-8").strip()

        if error:
            print(colored(f"Error creating CIFS share '{share_name}': {error}", "red"))
        elif "share already exists" in output.lower():
            print(colored(f"{dr_cluster_name}: CIFS share '{share_name}' already exists for volume '{volume_name}'.", "yellow"))
        elif "Operation succeeded" in output:
            print(colored(f"{dr_cluster_name}: CIFS share '{share_name}' successfully created for volume '{volume_name}'.", "green"))

    except Exception as e:
        print(colored(f"Exception occurred while creating CIFS share '{share_name}' for volume '{volume_name}': {str(e)}", "red"))

# Function to perform "recovery" action on all volumes
def perform_recovery(ssh, prod_ssh, vserver, prod_vserver, volumes, dr_cluster_name, prod_cluster_name, app_name, user, timezone):
    print(colored(f"\nStarting recovery process for all volumes in the app.", "cyan"))

    # Display the steps and commands to be executed first
    print("\nThe following steps will be performed:\n")

    # Step 1: SnapMirror Update
    print(colored(f"Step 1: SnapMirror Update @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror update -destination-path {vserver}:{volume_name}")

    # Step 2: SnapMirror Quiesce
    print(colored(f"\nStep 2: SnapMirror Quiesce @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror quiesce -destination-path {vserver}:{volume_name}")

    # Step 3: SnapMirror Break
    print(colored(f"\nStep 3: SnapMirror Break @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror break -destination-path {vserver}:{volume_name}")

    # Step 4: Unmount and Offline Volumes @prod_cluster
    print(colored(f"\nStep 4: Unmount and Offline Volumes @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"volume unmount -vserver {prod_vserver} -volume {volume_name}")
        print(f"volume offline -vserver {prod_vserver} -volume {volume_name}")

    # Step 5: Mount and CIFS Share Creation @dr_cluster
    print(colored(f"\nStep 5: Mount and CIFS Share Creation @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"volume mount -vserver {vserver} -volume {volume_name} -junction-path /{volume_name}")

        # Check for qtrees and create CIFS shares for each qtree
        if 'qtrees' in volume_data:
            for qtree in volume_data['qtrees']:
                print(f"cifs share create -vserver {vserver} -share-name {qtree['share_name']} -path /{volume_name}/{qtree['qtree_name']}")
        else:
            # If no qtrees, check if a direct share exists for the volume
            share_name = volume_data.get('share_name', None)
            if share_name:
                print(f"cifs share create -vserver {vserver} -share-name {share_name} -path /{volume_name}")
            else:
                print(f"Skipping CIFS share creation for volume '{volume_name}' as no share_name is associated.")

    # Ask for confirmation before proceeding
    confirm = input(colored("\nDo you want to proceed with the recovery process (yes/no)? ", "cyan")).lower()
    if confirm != 'yes':
        print(colored("Recovery process aborted.", "red"))
        return

    # Step 1: SnapMirror Update
    print(colored(f"\nStep 1: SnapMirror Update @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"{dr_cluster_name}: Updating SnapMirror for volume {volume_name}")
        perform_snapmirror_action(ssh, vserver, volume_name, "update", dr_cluster_name)
        print(colored(f"{dr_cluster_name}: SnapMirror update completed for volume {volume_name}.", "green"))


    # Wait for volumes to be in 'Idle' status
    print("\nWaiting for all volumes to be in 'Idle' status after update...\n")
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        wait_for_status(ssh, vserver, volume_name, "idle", dr_cluster_name, "Still transferring")

    # Step 2: SnapMirror Quiesce
    print(colored(f"\nStep 2: SnapMirror Quiesce @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"{dr_cluster_name}: Quiescing SnapMirror for volume {volume_name}")
        perform_snapmirror_action(ssh, vserver, volume_name, "quiesce", dr_cluster_name)
        print(colored(f"{dr_cluster_name}: SnapMirror quiesced for volume {volume_name}.", "green"))

    # Wait for volumes to be in 'Quiesced' status
    print("\nWaiting for all volumes to be in 'Quiesced' status after quiesce...\n")
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        wait_for_status(ssh, vserver, volume_name, "quiesced", dr_cluster_name, "Still quiescing")

    # Step 3: SnapMirror Break
    print(colored(f"\nStep 3: SnapMirror Break @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"{dr_cluster_name}: Breaking SnapMirror for volume {volume_name}")
        perform_snapmirror_action(ssh, vserver, volume_name, "break", dr_cluster_name)
        print(colored(f"{dr_cluster_name}: SnapMirror break completed for volume {volume_name}.", "green"))

    # Step 4: Unmount and Offline Volumes @prod_cluster
    print(colored(f"\nStep 4: Unmount and Offline Volumes @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"{prod_cluster_name}: Unmounting volume {volume_name}")
        unmount_and_offline_volume(prod_ssh, prod_vserver, volume_name, prod_cluster_name)

    # Step 5: Mount and CIFS Share Creation @dr_cluster
    print(colored(f"\nStep 5: Mount and CIFS Share Creation @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"{dr_cluster_name}: Mounting volume '{volume_name}' at junction path '/{volume_name}'...")
        mount_command = f"volume mount -vserver {vserver} -volume {volume_name} -junction-path /{volume_name}"
        stdin, stdout, stderr = ssh.exec_command(mount_command)
        stdout.read()
        print(colored(f"{dr_cluster_name}: Volume '{volume_name}' successfully mounted.", "green"))

        # Check for qtrees and create CIFS shares for each qtree
        if 'qtrees' in volume_data:
            for qtree in volume_data['qtrees']:
                share_name = qtree['share_name']
                qtree_name = qtree['qtree_name']
                share_command = f"cifs share create -vserver {vserver} -share-name {share_name} -path /{volume_name}/{qtree_name}"
                print(f"{dr_cluster_name}: Creating CIFS share '{share_name}' for qtree '{qtree_name}' under volume '{volume_name}'...")
                stdin, stdout, stderr = ssh.exec_command(share_command)
                stdout.read()
                print(colored(f"{dr_cluster_name}: CIFS share '{share_name}' successfully created for qtree '{qtree_name}' under volume '{volume_name}'.", "green"))
        else:
            # If no qtrees, check if a direct share exists for the volume
            share_name = volume_data.get('share_name', None)
            if share_name:
                share_command = f"cifs share create -vserver {vserver} -share-name {share_name} -path /{volume_name}"
                print(f"{dr_cluster_name}: Creating CIFS share '{share_name}' for volume '{volume_name}'...")
                stdin, stdout, stderr = ssh.exec_command(share_command)
                stdout.read()
                print(colored(f"{dr_cluster_name}: CIFS share '{share_name}' successfully created for volume '{volume_name}'.", "green"))
            else:
                print(f"Skipping CIFS share creation for volume '{volume_name}' as no share_name is associated.")

    save_recent_action(app_name, "recovery", user, timezone)

# Function to perform "recovery-extended" action on all volumes
def perform_recovery_extended(prod_ssh, dr_ssh, prod_vserver, dr_vserver, volumes, prod_cluster_name, dr_cluster_name, app_name, user, timezone):
    print(colored(f"\nStarting recovery-extended #reverse-replication-post-tvt for all volumes in the app.", "cyan"))

    # Display the steps and commands to be executed first
    print("\nThe following steps will be performed:\n")

    # Step 1: Bring PROD volumes online
    print(colored(f"Step 1: Bring PROD volumes online @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"vol online -vserver {prod_vserver} -volume {volume_name}")

    # Step 2: Create SnapMirror link @ new destination in PROD
    print(colored(f"\nStep 2: Create SnapMirror link @ new destination @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        # Use the schedule and policy details fetched from the SnapMirror table
        schedule = volume_data['snapmirror_details']['schedule']
        policy = volume_data['snapmirror_details']['policy']
        print(f"snapmirror create -source-path {dr_vserver}:{volume_name} -destination-path {prod_vserver}:{volume_name} -policy {policy} -schedule {schedule}")

    # Step 3: Resync DR to PROD in PROD
    print(colored(f"\nStep 3: Resync DR to PROD @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror resync -destination-path {prod_vserver}:{volume_name}")

    # Ask for confirmation before proceeding
    confirm = input(colored("\nDo you want to proceed with the recovery-extended process (yes/no)? ", "cyan")).lower()
    if confirm != 'yes':
        print(colored("Recovery-extended process aborted.", "red"))
        return  # Exit without performing actions or recording anything

    # Execute steps after confirmation
    # Step 1: Bring PROD volumes online
    print(colored(f"\nStep 1: Bringing PROD volumes online @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"vol online -vserver {prod_vserver} -volume {volume_name}"
        stdin, stdout, stderr = prod_ssh.exec_command(command)
        stdout.read()
        print(colored(f"{prod_cluster_name}: Volume '{volume_name}' is now online.", "green"))

    # Step 2: Create SnapMirror link @ new destination in PROD
    print(colored(f"\nStep 2: Creating SnapMirror link @ new destination @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        schedule = volume_data['snapmirror_details']['schedule']
        policy = volume_data['snapmirror_details']['policy']
        command = f"snapmirror create -source-path {dr_vserver}:{volume_name} -destination-path {prod_vserver}:{volume_name} -policy {policy} -schedule {schedule}"
        stdin, stdout, stderr = prod_ssh.exec_command(command)
        stdout.read()
        print(colored(f"{prod_cluster_name}: SnapMirror link created for volume '{volume_name}' with policy '{policy}' and schedule '{schedule}'.", "green"))

    # Step 3: Resync DR to PROD in PROD
    print(colored(f"\nStep 3: Resyncing DR to PROD @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"snapmirror resync -destination-path {prod_vserver}:{volume_name}"
        stdin, stdout, stderr = prod_ssh.exec_command(command)
        stdout.read()
        print(colored(f"{prod_cluster_name}: SnapMirror resync completed for volume '{volume_name}'.", "green"))

    # Record the action only after successful confirmation and execution
    save_recent_action(app_name, "recovery-extended", user, timezone)


# Function to perform "restoration-extended" action on all volumes
def perform_restoration_extended(prod_ssh, ssh, prod_vserver, dr_vserver, volumes, prod_cluster_name, dr_cluster_name, app_name, user, timezone):
    print(colored(f"\nStarting restoration-extended for all volumes in the app.", "cyan"))
    
    # Display the steps and commands to be executed first
    print("\nThe following steps will be performed:\n")

    # Step 1: Delete share in DR for each volume/qtree
    print(colored(f"Step 1: Delete CIFS shares in DR @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        
        if 'qtrees' in volume_data:
            for qtree in volume_data['qtrees']:
                share_name = qtree['share_name']
                print(f"cifs share delete -vserver {dr_vserver} -share-name {share_name}")
        else:
            # Direct share associated with the volume itself
            share_name = volume_data.get('share_name', None)
            if share_name:
                print(f"cifs share delete -vserver {dr_vserver} -share-name {share_name}")
            else:
                print(colored(f"Skipping CIFS share deletion for volume '{volume_name}' as no share_name is associated.", "yellow"))


    # Step 2: Update the replication from DR to PROD
    print(colored(f"\nStep 2: Update replication from DR to PROD @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror update -destination-path {prod_vserver}:{volume_name}")

    # Step 3: SnapMirror Quiesce in PROD
    print(colored(f"\nStep 3: SnapMirror Quiesce in PROD @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror quiesce -destination-path {prod_vserver}:{volume_name}")

    # Step 4: SnapMirror Break in PROD
    print(colored(f"\nStep 4: SnapMirror Break in PROD @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror break -destination-path {prod_vserver}:{volume_name}")

    # Step 5: Mount PROD volume and create CIFS share if not available
    print(colored(f"\nStep 5: Mount PROD volume and create CIFS share if not available @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"vol mount -vserver {prod_vserver} -volume {volume_name} -junction-path /{volume_name}")

        if 'qtrees' in volume_data:
            for qtree in volume_data['qtrees']:
                share_name = qtree['share_name']
                qtree_name = qtree['qtree_name']
                print(f"cifs share show -vserver {prod_vserver} -share-name {share_name}")
                print(f"If no share exists, create one:")
                print(f"cifs share create -vserver {prod_vserver} -share-name {share_name} -path /{volume_name}/{qtree_name}")
        else:
            # If no qtrees, check if a direct share exists for the volume
            share_name = volume_data.get('share_name', None)
            if share_name:
                print(f"cifs share show -vserver {prod_vserver} -share-name {share_name}")
                print(f"If no share exists, create one:")
                print(f"cifs share create -vserver {prod_vserver} -share-name {share_name} -path /{volume_name}")

    # Step 6: Check if PROD to DR replication link is created, if not, create it
    print(colored(f"\nStep 8: Check and create PROD to DR replication link @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        
        # Fetch existing details for policy and schedule from the PROD to DR replication
        prod_to_dr_details = fetch_snapmirror_details(ssh, dr_vserver, volume_name)
        policy = prod_to_dr_details.get("policy", "N/A")
        schedule = prod_to_dr_details.get("schedule", "N/A")
        
        # If policy and schedule are N/A, fallback to DR to PROD replication details
        if policy == "N/A" or schedule == "N/A":
            dr_to_prod_details = fetch_dr_to_prod_snapmirror_details(prod_ssh, prod_vserver, volume_name)
            if dr_to_prod_details:
                policy = dr_to_prod_details.get("policy", policy)
                schedule = dr_to_prod_details.get("schedule", schedule)

        # Only create SnapMirror if not already present and if policy and schedule are available
        if prod_to_dr_details["state"] == "N/A" and policy != "N/A" and schedule != "N/A":
            print(f"snapmirror create -source-path {prod_vserver}:{volume_name} -destination-path {dr_vserver}:{volume_name} -policy {policy} -schedule {schedule}")
        else:
            print(f"Replication link already exists for {volume_name}, skipping creation.")

    # Step 7: Delete DR to PROD sync
    print(colored(f"\nStep 6: Delete DR to PROD sync @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror delete -destination-path {prod_vserver}:{volume_name}")

    # Step 8: Unmount and take DR volume offline
    print(colored(f"\nStep 7: Unmount and take DR volume offline @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"vol unmount -vserver {dr_vserver} -volume {volume_name}")
        print(f"vol offline -vserver {dr_vserver} -volume {volume_name}")

    # Ask for confirmation before proceeding
    confirm = input(colored("\nDo you want to proceed with the restoration-extended process (yes/no)? ", "cyan")).lower()
    if confirm != 'yes':
        print(colored("Restoration-extended process aborted.", "red"))
        return

    # Execute each step and capture the output
    for volume_data in volumes:
        volume_name = volume_data['volume_name']

    # Step 1: Delete share in DR @dr_cluster
    print(colored(f"\nStep 1: Delete CIFS shares in DR @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        if 'qtrees' in volume_data:
            for qtree in volume_data['qtrees']:
                share_name = qtree['share_name']
                delete_share_command = f"cifs share delete -vserver {dr_vserver} -share-name {share_name}"
                stdin, stdout, stderr = ssh.exec_command(delete_share_command)
                output = stdout.read().decode("utf-8").strip()
                error = stderr.read().decode("utf-8").strip()
                if error:
                    print(colored(f"Error deleting CIFS share '{share_name}': {error}", "red"))
                else:
                    print(colored(f"{dr_cluster_name}: CIFS share '{share_name}' successfully deleted.", "green"))
        else:
            share_name = volume_data.get('share_name')
            if share_name:
                delete_share_command = f"cifs share delete -vserver {dr_vserver} -share-name {share_name}"
                stdin, stdout, stderr = ssh.exec_command(delete_share_command)
                output = stdout.read().decode("utf-8").strip()
                error = stderr.read().decode("utf-8").strip()
                if error:
                    print(colored(f"Error deleting CIFS share '{share_name}': {error}", "red"))
                else:
                    print(colored(f"{dr_cluster_name}: CIFS share '{share_name}' successfully deleted.", "green"))
            else:
                print(f"Skipping CIFS share creation for volume '{volume_name}' as no share_name is associated.")

    # Step 2: Update the replication of DR to PROD
    print(colored(f"\nStep 2: Updating the replication of DR to PROD @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"snapmirror update -destination-path {prod_vserver}:{volume_name}"
        stdin, stdout, stderr = prod_ssh.exec_command(command)
        stdout.read()
        print(colored(f"{prod_cluster_name}: SnapMirror update completed for volume {volume_name}.", "green"))

    # Wait for all volumes to be in 'Idle' status
    print("\nWaiting for all volumes to be in 'Idle' status after update...\n")
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        wait_for_status(prod_ssh, prod_vserver, volume_name, "idle", prod_cluster_name, "Still updating")

    # Step 3: SnapMirror Quiesce in PROD
    print(colored(f"\nStep 3: SnapMirror Quiesce in PROD @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"snapmirror quiesce -destination-path {prod_vserver}:{volume_name}"
        stdin, stdout, stderr = prod_ssh.exec_command(command)
        stdout.read()
        print(colored(f"{prod_cluster_name}: SnapMirror quiesced for volume {volume_name}.", "green"))

    # Wait for all volumes to be in 'Quiesced' status
    print("\nWaiting for all volumes to be in 'Quiesced' status after quiesce...\n")
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        wait_for_status(prod_ssh, prod_vserver, volume_name, "quiesced", prod_cluster_name, "Still quiescing")

    # Step 4: SnapMirror Break in PROD
    print(colored(f"\nStep 4: SnapMirror Break in PROD @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"snapmirror break -destination-path {prod_vserver}:{volume_name}"
        stdin, stdout, stderr = prod_ssh.exec_command(command)
        stdout.read()
        print(colored(f"{prod_cluster_name}: SnapMirror break completed for volume {volume_name}.", "green"))

    # Step 5: Mount PROD volume and create CIFS share if not available
    print(colored(f"\nStep 5: Mounting PROD volume and creating CIFS share if not available @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        junction_path = f"/{volume_name}"
        command_mount = f"vol mount -vserver {prod_vserver} -volume {volume_name} -junction-path {junction_path}"
        stdin, stdout, stderr = prod_ssh.exec_command(command_mount)
        stdout.read()
        print(colored(f"{prod_cluster_name}: Volume '{volume_name}' successfully mounted.", "green"))

        # Check for CIFS share
        share_name = volume_data.get('share_name')
        if share_name:
            command_check_share = f"cifs share show -vserver {prod_vserver} -share-name {share_name}"
            stdin, stdout, stderr = prod_ssh.exec_command(command_check_share)
            output = stdout.read().decode("utf-8").strip()
            if not output:
                print(colored(f"{prod_cluster_name}: No CIFS share '{share_name}' exists, creating one...", "yellow"))
                command_create_share = f"cifs share create -vserver {prod_vserver} -share-name {share_name} -path {junction_path}"
                stdin, stdout, stderr = prod_ssh.exec_command(command_create_share)
                stdout.read()
                print(colored(f"{prod_cluster_name}: CIFS share '{share_name}' successfully created.", "green"))

    # Step 6: Execute PROD to DR replication creation if missing
    print(colored(f"\nStep 8: Checking and creating PROD to DR replication link @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        prod_to_dr_details = fetch_snapmirror_details(ssh, dr_vserver, volume_name)
        
        # Fetch policy and schedule with fallback to DR to PROD if needed
        policy = prod_to_dr_details.get("policy", "N/A")
        schedule = prod_to_dr_details.get("schedule", "N/A")
        if policy == "N/A" or schedule == "N/A":
            dr_to_prod_details = fetch_dr_to_prod_snapmirror_details(prod_ssh, prod_vserver, volume_name)
            if dr_to_prod_details:
                policy = dr_to_prod_details.get("policy", policy)
                schedule = dr_to_prod_details.get("schedule", schedule)

        # Create SnapMirror link if it doesn’t exist
        if prod_to_dr_details["state"] == "N/A" and policy != "N/A" and schedule != "N/A":
            create_command = f"snapmirror create -source-path {prod_vserver}:{volume_name} -destination-path {dr_vserver}:{volume_name} -policy {policy} -schedule {schedule}"
            stdin, stdout, stderr = ssh.exec_command(create_command)
            stdout.read()
            print(colored(f"{dr_cluster_name}: SnapMirror link created for volume '{volume_name}' with policy '{policy}' and schedule '{schedule}'.", "green"))
        else:
            print(colored(f"{dr_cluster_name}: PROD to DR replication link already exists for volume '{volume_name}'. Skipping creation.", "yellow"))

    # Step 7: Delete DR to PROD sync
    print(colored(f"\nStep 6: Deleting DR to PROD sync @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"snapmirror delete -destination-path {prod_vserver}:{volume_name}"
        stdin, stdout, stderr = prod_ssh.exec_command(command)
        stdout.read()
        print(colored(f"{prod_cluster_name}: SnapMirror deleted for volume {volume_name}.", "green"))

    # Step 8: Unmount and Take DR volume offline temporarily @dr_cluster
    print(colored(f"\nStep 7: Unmount and Take DR volume offline temporarily @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"{dr_cluster_name}: Unmounting volume '{volume_name}'")
        unmount_command = f"volume unmount -vserver {dr_vserver} -volume {volume_name}"
        stdin, stdout, stderr = ssh.exec_command(unmount_command)
        stdout.read()
        
        print(f"{dr_cluster_name}: Taking volume '{volume_name}' offline")
        offline_command = f"volume offline -vserver {dr_vserver} -volume {volume_name}"
        stdin, stdout, stderr = ssh.exec_command(offline_command)
        stdout.read()

    save_recent_action(app_name, "restoration-extended", user, timezone)

# Function to perform "restoration-flip-flop" action on all volumes in the app
def perform_restoration_flip_flop(ssh_prod, ssh_dr, vserver_prod, vserver_dr, volumes, prod_cluster_name, dr_cluster_name, app_name, user, timezone):
    print(colored(f"\nStarting restoration-flip-flop #no-replication for all volumes in the app.", "cyan"))

    # Display the steps and commands to be executed first
    print("\nThe following steps will be performed:\n")

    # Step 1: Bring PROD volumes online
    print(colored(f"Step 1: Bring PROD volumes online @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"vol online -vserver {vserver_prod} -volume {volume_name}")

    # Step 2: Mount PROD volumes junction path
    print(colored(f"\nStep 2: Mount PROD volumes junction path @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"vol mount -vserver {vserver_prod} -volume {volume_name} -junction-path /{volume_name}")

    # Step 3: Verify or create CIFS shares in PROD
    print(colored(f"\nStep 3: Verify CIFS shares in PROD @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        if 'qtrees' in volume_data:
            # For volumes with qtrees, verify or create CIFS shares for each qtree
            for qtree in volume_data['qtrees']:
                share_name = qtree['share_name']
                qtree_name = qtree['qtree_name']
                print(f"cifs share show -vserver {vserver_prod} -share-name {share_name}")
                print(f"If no share exists, create one:")
                print(f"cifs share create -vserver {vserver_prod} -share-name {share_name} -path /{volume_name}/{qtree_name}")
        else:
            # If no qtrees, verify or create a CIFS share for the volume itself
            share_name = volume_data.get('share_name', None)
            if share_name:
                print(f"cifs share show -vserver {vserver_prod} -share-name {share_name}")
                print(f"If no share exists, create one:")
                print(f"cifs share create -vserver {vserver_prod} -share-name {share_name} -path /{volume_name}")
            else:
                print(f"Skipping CIFS share verification for volume '{volume_name}' as no share_name is associated.")

    # Step 4: Unmount and offline DR volumes
    print(colored(f"\nStep 4: Unmount and offline DR volumes @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"volume unmount -vserver {vserver_dr} -volume {volume_name}")
        print(f"volume offline -vserver {vserver_dr} -volume {volume_name}")

    # Ask for confirmation before proceeding
    confirm = input(colored("\nDo you want to proceed with the restoration-flip-flop task for all volumes (yes/no)? ", "cyan")).lower()
    if confirm != 'yes':
        print(colored("Restoration-flip-flop process aborted.", "red"))
        return

    # Step 1: Bring PROD volumes online
    print(colored(f"\nStep 1: Bringing PROD volumes online @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"{prod_cluster_name}: Running online command for volume {volume_name}")
        command = f"vol online -vserver {vserver_prod} -volume {volume_name}"
        stdin, stdout, stderr = ssh_prod.exec_command(command)
        stdout.read()
        print(colored(f"{prod_cluster_name}: Volume '{volume_name}' is now online.", "green"))

    # Step 2: Mount PROD volumes junction path
    print(colored(f"\nStep 2: Mounting PROD volumes junction path @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"{prod_cluster_name}: Mounting volume '{volume_name}'...")
        command = f"vol mount -vserver {vserver_prod} -volume {volume_name} -junction-path /{volume_name}"
        stdin, stdout, stderr = ssh_prod.exec_command(command)
        stdout.read()
        print(colored(f"{prod_cluster_name}: Volume '{volume_name}' successfully mounted.", "green"))

    # Step 3: Verify or create CIFS shares in PROD
    print(colored(f"\nStep 3: Verifying or creating CIFS shares in PROD @ {prod_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        if 'qtrees' in volume_data:
            # For volumes with qtrees, verify or create CIFS shares for each qtree
            for qtree in volume_data['qtrees']:
                share_name = qtree['share_name']
                qtree_name = qtree['qtree_name']
                print(f"{prod_cluster_name}: Verifying CIFS share '{share_name}' for qtree '{qtree_name}' under volume '{volume_name}'...")
                command = f"cifs share show -vserver {vserver_prod} -share-name {share_name}"
                stdin, stdout, stderr = ssh_prod.exec_command(command)
                output = stdout.read().decode("utf-8").strip()
                if output:
                    print(output)
                else:
                    print(colored(f"No CIFS share '{share_name}' exists, creating one...", "yellow"))
                    command = f"cifs share create -vserver {vserver_prod} -share-name {share_name} -path /{volume_name}/{qtree_name}"
                    stdin, stdout, stderr = ssh_prod.exec_command(command)
                    stdout.read()
        else:
            # If no qtrees, verify or create CIFS share for the volume itself
            share_name = volume_data.get('share_name', None)
            if share_name:
                print(f"{prod_cluster_name}: Verifying CIFS share '{share_name}'...")
                command = f"cifs share show -vserver {vserver_prod} -share-name {share_name}"
                stdin, stdout, stderr = ssh_prod.exec_command(command)
                output = stdout.read().decode("utf-8").strip()
                if output:
                    print(output)
                else:
                    print(colored(f"No CIFS share '{share_name}' exists, creating one...", "yellow"))
                    command = f"cifs share create -vserver {vserver_prod} -share-name {share_name} -path /{volume_name}"
                    stdin, stdout, stderr = ssh_prod.exec_command(command)
                    stdout.read()
            else:
                print(f"Skipping CIFS share verification for volume '{volume_name}' as no share_name is associated.")

    # Step 4: Unmount and offline DR volumes
    print(colored(f"\nStep 4: Unmounting and offlining DR volumes @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"{dr_cluster_name}: Unmounting volume '{volume_name}'")
        command = f"volume unmount -vserver {vserver_dr} -volume {volume_name}"
        stdin, stdout, stderr = ssh_dr.exec_command(command)
        stdout.read()

        print(f"{dr_cluster_name}: Offlining volume '{volume_name}'")
        command = f"volume offline -vserver {vserver_dr} -volume {volume_name}"
        stdin, stdout, stderr = ssh_dr.exec_command(command)
        stdout.read()

    save_recent_action(app_name, "restoration-flip-flop", user, timezone)

# Function to perform "restoration-post-tvt" action on all volumes
def perform_restoration_post_tvt(ssh, prod_ssh, dr_vserver, prod_vserver, volumes, dr_cluster_name, prod_cluster_name, app_name, user, timezone):
    print(colored(f"\nStarting restoration-post-tvt for all volumes in the app.", "cyan"))

    # Display the steps and commands to be executed first
    print("\nThe following steps will be performed:\n")

    # Step 1: Bring volumes online in DR
    print(colored(f"Step 1: Bring volumes online in DR @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"vol online -vserver {dr_vserver} -volume {volume_name}")

    # Step 2: Check if PROD to DR replication link exists; if not, create one
    print(colored(f"\nStep 2: Check for and potentially create PROD to DR replication link @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        # Fetch existing details for policy and schedule
        prod_to_dr_details = fetch_snapmirror_details(ssh, dr_vserver, volume_name)
        policy = prod_to_dr_details.get("policy", "N/A")
        schedule = prod_to_dr_details.get("schedule", "N/A")
        
        # If policy and schedule are N/A, fetch from DR to PROD replication details
        if policy == "N/A" or schedule == "N/A":
            dr_to_prod_details = fetch_dr_to_prod_snapmirror_details(prod_ssh, prod_vserver, volume_name)
            if dr_to_prod_details:
                policy = dr_to_prod_details.get("policy", policy)
                schedule = dr_to_prod_details.get("schedule", schedule)
        
        # Check if replication exists by policy and schedule
        if policy == "N/A" or schedule == "N/A":
            print(colored(f"Skipping SnapMirror creation for {volume_name} as policy/schedule details are missing.", "red"))
            continue
        
        # Only create SnapMirror if not already present
        if prod_to_dr_details["state"] == "N/A":
            print(f"snapmirror create -source-path {prod_vserver}:{volume_name} -destination-path {dr_vserver}:{volume_name} -policy {policy} -schedule {schedule}")
        else:
            print(f"Replication link already exists for {volume_name}, skipping creation.")


    # Step 3: Delete CIFS shares in DR (if any)
    print(colored(f"\nStep 3: Delete CIFS shares in DR (if any) @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        if 'qtrees' in volume_data:
            for qtree in volume_data['qtrees']:
                share_name = qtree['share_name']
                print(f"cifs share delete -vserver {dr_vserver} -share-name {share_name}")
        elif 'share_name' in volume_data:
            share_name = volume_data['share_name']
            print(f"cifs share delete -vserver {dr_vserver} -share-name {share_name}")
        else:
            print(f"No CIFS shares associated with volume '{volume_name}', skipping.")

    # Step 4: Resync SnapMirror from PROD to DR
    print(colored(f"\nStep 4: Resync SnapMirror from PROD to DR @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror resync -destination-path {dr_vserver}:{volume_name}")

    # Ask for confirmation before proceeding
    confirm = input(colored("\nDo you want to proceed with the restoration-post-tvt process (yes/no)? ", "cyan")).lower()
    if confirm != 'yes':
        print(colored("Restoration-post-tvt process aborted.", "red"))
        return

    # Execute each step and capture the output
    # Step 1: Bring volumes online in DR
    print(colored(f"\nStep 1: Bringing volumes online in DR @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"vol online -vserver {dr_vserver} -volume {volume_name}"
        stdin, stdout, stderr = ssh.exec_command(command)
        stdout.read()
        print(colored(f"{dr_cluster_name}: Volume '{volume_name}' is now online.", "green"))

    # Step 2: Check if PROD to DR replication link exists; if not, create one
    print(colored(f"\nStep 2: Checking and creating replication link if needed @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        snapmirror_details = fetch_snapmirror_details(ssh, dr_vserver, volume_name)
        if snapmirror_details['state'] == 'N/A':
            policy = volume_data['snapmirror_details']['policy']
            schedule = volume_data['snapmirror_details']['schedule']
            command = f"snapmirror create -source-path {prod_vserver}:{volume_name} -destination-path {dr_vserver}:{volume_name} -policy {policy} -schedule {schedule}"
            stdin, stdout, stderr = ssh.exec_command(command)
            stdout.read()
            print(colored(f"{dr_cluster_name}: SnapMirror link created for volume '{volume_name}'.", "green"))
        else:
            print(colored(f"{dr_cluster_name}: Replication link already exists for volume '{volume_name}', skipping creation.", "yellow"))

    # Step 3: Delete CIFS shares in DR (if any)
    print(colored(f"\nStep 3: Deleting CIFS shares in DR @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        if 'qtrees' in volume_data:
            for qtree in volume_data['qtrees']:
                share_name = qtree['share_name']
                delete_command = f"cifs share delete -vserver {dr_vserver} -share-name {share_name}"
                stdin, stdout, stderr = ssh.exec_command(delete_command)
                stdout.read()
                print(colored(f"{dr_cluster_name}: CIFS share '{share_name}' deleted.", "green"))
        elif 'share_name' in volume_data:
            share_name = volume_data['share_name']
            delete_command = f"cifs share delete -vserver {dr_vserver} -share-name {share_name}"
            stdin, stdout, stderr = ssh.exec_command(delete_command)
            stdout.read()
            print(colored(f"{dr_cluster_name}: CIFS share '{share_name}' deleted.", "green"))
        else:
            print(colored(f"No CIFS share associated with volume '{volume_name}', skipping deletion.", "yellow"))

    # Step 4: Resync SnapMirror from PROD to DR
    print(colored(f"\nStep 4: Resyncing SnapMirror from PROD to DR @ {dr_cluster_name}", "cyan"))
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"snapmirror resync -destination-path {dr_vserver}:{volume_name}"
        stdin, stdout, stderr = ssh.exec_command(command)
        stdout.read()
        print(colored(f"{dr_cluster_name}: SnapMirror resync completed for volume '{volume_name}'.", "green"))

    save_recent_action(app_name, "restoration-post-tvt", user, timezone)

def perform_quiesce(ssh, vserver, volumes, cluster_name, app_name, user, timezone):
    print(colored(f"\nStarting quiesce for all volumes in the app on {cluster_name}.", "cyan"))

    # Display the steps and commands to be executed first
    print("\nThe following steps will be performed:\n")
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror quiesce -destination-path {vserver}:{volume_name}")

    # Ask for confirmation before proceeding
    confirm = input(colored("\nDo you want to proceed with the quiesce process (yes/no)? ", "cyan")).lower()
    if confirm != 'yes':
        print(colored("Quiesce process aborted.", "red"))
        return  # Exit without performing actions or recording anything

    # Execute steps after confirmation
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"snapmirror quiesce -destination-path {vserver}:{volume_name}"
        stdin, stdout, stderr = ssh.exec_command(command)
        stdout.read()
        print(colored(f"{cluster_name}: SnapMirror quiesced for volume '{volume_name}'.", "green"))

    # Record the action only after completion
    save_recent_action(app_name, "quiesce", user, timezone)

def perform_break(ssh, vserver, volumes, cluster_name, app_name, user, timezone):
    print(colored(f"\nStarting break for all volumes in the app on {cluster_name}.", "cyan"))

    # Display the steps and commands to be executed first
    print("\nThe following steps will be performed:\n")
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror break -destination-path {vserver}:{volume_name}")

    # Ask for confirmation before proceeding
    confirm = input(colored("\nDo you want to proceed with the break process (yes/no)? ", "cyan")).lower()
    if confirm != 'yes':
        print(colored("Break process aborted.", "red"))
        return  # Exit without performing actions or recording anything

    # Execute steps after confirmation
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"snapmirror break -destination-path {vserver}:{volume_name}"
        stdin, stdout, stderr = ssh.exec_command(command)
        stdout.read()
        print(colored(f"{cluster_name}: SnapMirror break completed for volume '{volume_name}'.", "green"))

    # Record the action only after completion
    save_recent_action(app_name, "break", user, timezone)

def perform_resync(ssh, vserver, volumes, cluster_name, app_name, user, timezone):
    print(colored(f"\nStarting resync for all volumes in the app on {cluster_name}.", "cyan"))

    # Display the steps and commands to be executed first
    print("\nThe following steps will be performed:\n")
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        print(f"snapmirror resync -destination-path {vserver}:{volume_name}")

    # Ask for confirmation before proceeding
    confirm = input(colored("\nDo you want to proceed with the resync process (yes/no)? ", "cyan")).lower()
    if confirm != 'yes':
        print(colored("Resync process aborted.", "red"))
        return  # Exit without performing actions or recording anything

    # Execute steps after confirmation
    for volume_data in volumes:
        volume_name = volume_data['volume_name']
        command = f"snapmirror resync -destination-path {vserver}:{volume_name}"
        stdin, stdout, stderr = ssh.exec_command(command)
        stdout.read()
        print(colored(f"{cluster_name}: SnapMirror resync completed for volume '{volume_name}'.", "green"))

    # Record the action only after completion
    save_recent_action(app_name, "resync", user, timezone)

# Function to perform "update" action on all volumes
def perform_update(ssh, prod_ssh, vserver, prod_vserver, volume_names, dr_cluster_name, prod_cluster_name, app_name, user, timezone):
    print("\nThe following updates will be performed:\n")

    # Step 1: Determine the active replication direction for each volume
    for volume_data in volume_names:
        volume_name = volume_data['volume_name']

        # Check PROD to DR replication
        prod_to_dr_details = fetch_snapmirror_details(ssh, vserver, volume_name)
        dr_to_prod_details = fetch_dr_to_prod_snapmirror_details(prod_ssh, prod_vserver, volume_name)

        if prod_to_dr_details["state"] == "Snapmirrored":
            print(f"Updating PROD to DR replication for volume '{volume_name}' on {dr_cluster_name}.")
            print(f"snapmirror update -destination-path {vserver}:{volume_name}")
        
        elif dr_to_prod_details and dr_to_prod_details["state"] == "Snapmirrored":
            print(f"Updating DR to PROD replication for volume '{volume_name}' on {prod_cluster_name}.")
            print(f"snapmirror update -destination-path {prod_vserver}:{volume_name}")

        else:
            print(f"No active SnapMirror link found for volume '{volume_name}', skipping update.")
    
    # Step 2: Confirm with the user to proceed
    confirm = input(colored("\nDo you want to proceed with the update for all volumes (yes/no)? ", "cyan")).lower()
    if confirm != 'yes':
        print(colored("Update process aborted.", "red"))
        return

    # Step 3: Execute the update action for each volume based on the active replication link direction
    for volume_data in volume_names:
        volume_name = volume_data['volume_name']
        # Recheck replication direction before performing action
        prod_to_dr_details = fetch_snapmirror_details(ssh, vserver, volume_name)
        dr_to_prod_details = fetch_dr_to_prod_snapmirror_details(prod_ssh, prod_vserver, volume_name)

        try:
            if prod_to_dr_details["state"] == "Snapmirrored":
                # Execute PROD to DR update
                perform_snapmirror_action(ssh, vserver, volume_name, "update", dr_cluster_name)
                print(colored(f"PROD to DR update succeeded for volume '{volume_name}' on {dr_cluster_name}.", "green"))

            elif dr_to_prod_details and dr_to_prod_details["state"] == "Snapmirrored":
                # Execute DR to PROD update
                perform_snapmirror_action(prod_ssh, prod_vserver, volume_name, "update", prod_cluster_name)
                print(colored(f"DR to PROD update succeeded for volume '{volume_name}' on {prod_cluster_name}.", "green"))

            else:
                print(colored(f"Skipping update for volume '{volume_name}' as no active SnapMirror link was found.", "yellow"))

            # Record the update action
            save_recent_action(app_name, "update", user, timezone)

        except Exception as e:
            print(colored(f"Error performing update for volume '{volume_name}': {e}", "red"))

# Define color list for apps
APP_COLORS = ["cyan", "yellow", "green", "magenta", "blue"]

# Function to display SnapMirror details in table format
def display_table(snapmirror_details_list, app_color_map):
    # Filter out entries where all relevant fields are marked as "N/A"
    valid_entries = [
        entry for entry in snapmirror_details_list 
        if not all(field == "N/A" for field in entry[2:6])  # Skip if State, Status, Lag Time, Schedule are all "N/A"
    ]

    if not valid_entries:
        print(colored("\n\n===== NO ACTIVE PROD TO DR REPLICATION FOUND =====\n", "yellow", attrs=["bold", "underline"]))
        return

    headers = ["App Name", "Volume Name", "State", "Status", "Lag Time", "Schedule", "Policy", "Recent Action", "User", "Timestamp"]
    time.sleep(1)
    print("\n")

    # Apply bold and color to headers
    header_colored = [colored(f"{header}", "white", attrs=["bold"]) for header in headers]

    # Loop through entries and apply consistent colors for each app
    table_data = []
    for entry in valid_entries:
        app_name = entry[0]

        # Assign a color to each app if not already assigned
        if app_name not in app_color_map:
            app_color_map[app_name] = APP_COLORS[len(app_color_map) % len(APP_COLORS)]
        app_color = app_color_map[app_name]

        # Format the entry with the app color and bold for the app name
        formatted_entry = [
            colored(f"{app_name}", app_color, attrs=["bold"]),  # App Name in bold with color
            *[f"{field}" for field in entry[1:]]
        ]
        table_data.append(formatted_entry)

    # Display the table with a box format
    print(colored("\n\n===== CURRENT PROD TO DR REPLICATION =====\n", "cyan", attrs=["bold", "underline"]))
    table = tabulate(table_data, headers=header_colored, tablefmt="fancy_grid", numalign="center", stralign="center")
    print(colored(table, "yellow"))

# Function to display DR-to-PROD SnapMirror replication table
def display_dr_to_prod_table(dr_to_prod_details_list, app_color_map):
    # Filter out any N/A or None entries
    active_replications = [entry for entry in dr_to_prod_details_list if entry is not None]

    if not active_replications:
        print(colored("\n\n===== NO ACTIVE DR TO PROD REPLICATION FOUND =====\n", "yellow", attrs=["bold", "underline"]))
        return

    headers = ["App Name", "Volume Name", "State", "Status", "Lag Time", "Schedule", "Policy", "Recent Action", "User", "Timestamp"]
    time.sleep(2)
    print("\n")

    # Apply bold formatting to headers
    header_colored = [colored(f"{header}", "white", attrs=["bold"]) for header in headers]

    # Process entries, assigning a consistent color per app
    table_data = []
    for entry in active_replications:
        app_name = entry[0]

        # Assign color if not already done
        if app_name not in app_color_map:
            app_color_map[app_name] = APP_COLORS[len(app_color_map) % len(APP_COLORS)]
        app_color = app_color_map[app_name]

        # Format the entry with the app color and bold the app name
        formatted_entry = [
            colored(f"{app_name}", app_color, attrs=["bold"]),
            *[f"{field}" for field in entry[1:]]
        ]
        table_data.append(formatted_entry)

    # Display the table with consistent colors for each app
    print(colored("\n\n===== CURRENT DR TO PROD REPLICATION =====\n", "cyan", attrs=["bold", "underline"]))
    table = tabulate(table_data, headers=header_colored, tablefmt="fancy_grid", numalign="center", stralign="center")
    print(colored(table, "yellow"))

# Function to show detailed information about the app
def show_app_details(app_entry):
    print("\n" + colored("App Details for", "cyan", attrs=["bold"]) + f" {app_entry['app_name']}:")
    print(colored(f"{'='*40}", "yellow"))
    print(colored(f"Prod Cluster      : {app_entry['prod_cluster']}", "yellow"))
    print(colored(f"DR Cluster        : {app_entry['dr_cluster']}", "yellow"))
    
    # Show any additional details if present
    details = app_entry.get('details', None)
    if details:
        print(colored(f"\nDetails:", "yellow"))
        print(colored(details, "yellow"))

    for volume in app_entry['volume_names']:
        source_path = f"{app_entry['prod_vserver']}:{volume['volume_name']}"
        destination_path = f"{app_entry['dr_vserver']}:{volume['volume_name']}"
        print(colored(f"\nVolume Name       : {volume['volume_name']}", "yellow"))
        print(colored(f"  Source Path     : {source_path}", "yellow"))
        print(colored(f"  Destination Path: {destination_path}", "yellow"))

        # Check if there are qtrees
        if 'qtrees' in volume:
            for qtree in volume['qtrees']:
                print(colored(f"    Qtree         : {qtree['qtree_name']}", "yellow"))
                print(colored(f"    CIFS Share    : {qtree['share_name']}", "yellow"))
        else:
            # If no qtrees, just show the CIFS share for the volume
            share_name = volume.get('share_name', None)
            if share_name:
                print(colored(f"  CIFS Share      : {share_name}", "yellow"))

    print(colored(f"{'='*40}\n", "yellow"))

# Function to read YAML input file
def read_input_yaml(file_path):
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

# Updated main function to add DR-to-PROD table
def main():
    yaml_file = 'snapmirror_input.yaml'
    input_data = read_input_yaml(yaml_file)

    username = input(colored("Enter the username: ", "cyan"))
    password = getpass(colored("Enter the password: ", "cyan"))

    user = os.getlogin()

    timezone = str(get_localzone())

    # Initialize app color map to track assigned colors for each app
    app_color_map = {}
#    recent_actions = load_recent_actions()  # Load recent actions on start
    cluster_connections = {}  # Cache SSH connections per cluster



    def get_or_create_connection(cluster):
        """Get or create SSH connection for the specified cluster."""
        if cluster not in cluster_connections:
            cluster_connections[cluster] = ssh_connect(cluster, username, password)
        return cluster_connections[cluster]

    # Function to load recent actions each time we display the table
    def load_recent_actions():
        """Load the recent actions log from the shared file."""
        try:
            with open(RECENT_ACTIONS_FILE, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Failed to load recent actions: {e}")
            return {}

    while True:
        # Load recent actions for the current session
        recent_actions = load_recent_actions()

        snapmirror_details_list = []
        dr_to_prod_details_list = []  # Reset list each iteration for fresh data

        def fetch_volume_data(entry):
            """Fetch SnapMirror details for each volume using multi-threading."""
            app_name = entry['app_name']
            dr_cluster = entry['dr_cluster']
            prod_cluster = entry['prod_cluster']
            vserver = entry['dr_vserver']
            prod_vserver = entry['prod_vserver']
            volume_names = entry['volume_names']

            # Maintain single SSH connections for clusters
            ssh = get_or_create_connection(dr_cluster)
            prod_ssh = get_or_create_connection(prod_cluster)
            dr_cluster_name = get_cluster_name(dr_cluster)
            prod_cluster_name = get_cluster_name(prod_cluster)

            # Fetch SnapMirror details for each volume under the app
            for volume_data in volume_names:
                volume_name = volume_data['volume_name']

                # Fetch SnapMirror details and add to volume_data
                snapmirror_details = fetch_snapmirror_details(ssh, vserver, volume_name)
                volume_data['snapmirror_details'] = snapmirror_details  # Attach details here

                recent_action = recent_actions.get(app_name, [{"action": "-", "user": "-", "timestamp": "-"}])[-1]  # Get recent action
                snapmirror_details_list.append([
                    app_name,
                    volume_name,
                    snapmirror_details["state"],
                    snapmirror_details["status"],
                    snapmirror_details["lag-time"],
                    snapmirror_details["schedule"],
                    snapmirror_details["policy"],
                    recent_action["action"],  # Show recent action
                    recent_action["user"],    # Show user who performed the action
                    recent_action["timestamp"] # Show timestamp of the action
                ])

                # Check for active DR-to-PROD replication
                dr_to_prod_details = fetch_dr_to_prod_snapmirror_details(prod_ssh, prod_vserver, volume_name)
                if dr_to_prod_details:
                    dr_to_prod_details_list.append([
                        app_name,
                        volume_name,
                        dr_to_prod_details["state"],
                        dr_to_prod_details["status"],
                        dr_to_prod_details["lag-time"],
                        dr_to_prod_details["schedule"],
                        dr_to_prod_details["policy"],
                        recent_action["action"],  # Add recent action to DR-to-PROD table
                        recent_action["user"],    # User who performed the action
                        recent_action["timestamp"] # Timestamp of the action
                    ])

        # Use threads to fetch data in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(fetch_volume_data, input_data)

        # Sort the lists by app name and volume name
        snapmirror_details_list.sort(key=lambda x: (x[0], x[1]))  # Sort by app_name (x[0]) and volume_name (x[1])
        dr_to_prod_details_list.sort(key=lambda x: (x[0], x[1]))  # Sort by app_name (x[0]) and volume_name (x[1])

        # Display the primary SnapMirror details table
        display_table(snapmirror_details_list, app_color_map)

        # Display the DR-to-PROD replication table if there are active replications
        display_dr_to_prod_table(dr_to_prod_details_list, app_color_map)

        # Ask for the app_name to take action on or type 'reload'
        app_name = input(colored("Enter the app name to action (or type 'reload' to refresh): ", "cyan"))

        if app_name == 'reload':
            continue  # Refresh and show tables again

        # Find the entry corresponding to the provided app name
        selected_entry = next((entry for entry in input_data if entry['app_name'] == app_name), None)

        if selected_entry:
            dr_cluster = selected_entry['dr_cluster']
            prod_cluster = selected_entry['prod_cluster']
            vserver = selected_entry['dr_vserver']
            prod_vserver = selected_entry['prod_vserver']
            volume_names = selected_entry['volume_names']

            # Use the cached SSH connections
            ssh = get_or_create_connection(dr_cluster)
            prod_ssh = get_or_create_connection(prod_cluster)
            dr_cluster_name = get_cluster_name(dr_cluster)
            prod_cluster_name = get_cluster_name(prod_cluster)

            # List the available actions per line
            print(colored("\nWhat action would you like to perform on the app?\n", "magenta", attrs=["bold"]))
            print(colored("1. show details", "white"))
            print(colored("1. update", "white"))
            print(colored("2. quiesce", "white"))
            print(colored("3. break", "white"))
            print(colored("4. resync", "white"))
            print(colored("5. recovery", "white"))
            print(colored("6. recovery-extended #reverse-replication-post-tvt", "white"))
            print(colored("7. restoration-extended", "white"))
            print(colored("8. restoration-flip-flop #no-replication", "white"))
            print(colored("9. restoration-post-tvt", "white"))

            action = input(colored("Choose the action (case sensitive): ", "cyan"))
#            user = getpass.getuser()  # Get the current user who is running the script

            if action == 'show details':
                show_app_details(selected_entry)
            elif action == 'recovery':
                perform_recovery(ssh, prod_ssh, vserver, prod_vserver, volume_names, dr_cluster_name, prod_cluster_name, app_name, user, timezone)
            elif action == 'recovery-extended':
                perform_recovery_extended(prod_ssh, ssh, prod_vserver, vserver, volume_names, prod_cluster_name, dr_cluster_name, app_name, user, timezone)
            elif action == 'restoration-extended':
                perform_restoration_extended(prod_ssh, ssh, prod_vserver, vserver, volume_names, prod_cluster_name, dr_cluster_name, app_name, user, timezone)
            elif action == 'restoration-flip-flop':
                perform_restoration_flip_flop(prod_ssh, ssh, prod_vserver, vserver, volume_names, prod_cluster_name, dr_cluster_name, app_name, user, timezone)
            elif action == 'restoration-post-tvt':
                perform_restoration_post_tvt(ssh, prod_ssh, vserver, prod_vserver, volume_names, dr_cluster_name, prod_cluster_name, app_name, user, timezone)
            elif action == 'update':
                perform_update(ssh, prod_ssh, vserver, prod_vserver, volume_names, dr_cluster_name, prod_cluster_name, app_name, user, timezone)
            elif action == 'quiesce':
                perform_quiesce(ssh, vserver, volume_names, dr_cluster_name, app_name, user, timezone)
            elif action == 'break':
                perform_break(ssh, vserver, volume_names, dr_cluster_name, app_name, user, timezone)
            elif action == 'resync':
                perform_resync(ssh, vserver, volume_names, dr_cluster_name, app_name, user, timezone)
            else:
                # Perform the action on all volumes under the selected app
                for volume_data in volume_names:
                    volume_name = volume_data['volume_name']
                    perform_snapmirror_action(ssh, vserver, volume_name, action, dr_cluster_name, timezone)
                if action in VALID_ACTIONS:
                    save_recent_action(app_name, action, user, timezone)  # Only save if action is valid
                else:
                    print(colored(f"Invalid action '{action}' specified. It will not be recorded.", "red"))

            #Reload recent actions to display updated info immediately
            recent_actions = load_recent_actions()

        else:
            print(colored(f"App '{app_name}' not found in input data.", "red"))

    # Clean up SSH connections after exiting the loop
    for ssh in cluster_connections.values():
        ssh.close()

if __name__ == "__main__":
    main()
