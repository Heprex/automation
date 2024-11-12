import getpass
import logging
import paramiko
import math
import json  # Import for JSON handling
from netapp_ontap import HostConnection
from netapp_ontap.resources import Volume, Aggregate, Svm, ExportPolicy, SnapshotPolicy
from netapp_ontap.error import NetAppRestError

# Set up logging for your script
logging.basicConfig(level=logging.WARNING)  # Only show warnings and errors
logger = logging.getLogger()

# Reduce verbosity for paramiko and netapp_ontap
paramiko.util.logging.getLogger("paramiko").setLevel(logging.WARNING)
logging.getLogger("netapp_ontap").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.ERROR)  # Suppress urllib3 warnings

def format_size(size_bytes):
    """Convert bytes to a human-readable format (GB)."""
    if size_bytes is not None:
        return size_bytes / (1024**3)  # Convert bytes to gigabytes (GB)
    return None

def get_aggregates(cluster, username, password):
    """Fetches and returns a list of aggregates from the ONTAP cluster."""
    try:
        with HostConnection(
            host=cluster,
            username=username,
            password=password,
            verify=False
        ):
            aggregates = list(Aggregate.get_collection())
            for aggregate in aggregates:
                aggregate.get()  # Fetch full details for each aggregate
            return aggregates
    except NetAppRestError as e:
        print(f"Error fetching aggregates: {e}")
        return []

def get_vservers(cluster, username, password):
    """Fetches and returns a list of SVMs (vservers) from the ONTAP cluster."""
    try:
        with HostConnection(
            host=cluster,
            username=username,
            password=password,
            verify=False
        ):
            vservers = list(Svm.get_collection())
            for vserver in vservers:
                vserver.get()  # Fetch full details for each SVM
            return vservers
    except NetAppRestError as e:
        print(f"Error fetching vservers: {e}")
        return []

def get_snapshot_policies(cluster, username, password):
    """Fetches and returns a list of snapshot policies from the ONTAP cluster,
    excluding those with '-DR' in their name. It also includes the associated SVM."""
    try:
        with HostConnection(
            host=cluster,
            username=username,
            password=password,
            verify=False
        ):
            snapshot_policies = list(SnapshotPolicy.get_collection(fields="name,svm"))
            # Exclude policies with '-DR' in the name
            snapshot_policies = [policy for policy in snapshot_policies if '-DR' not in policy.name]
            return snapshot_policies
    except NetAppRestError as e:
        print(f"Error fetching snapshot policies: {e}")
        return []

def get_cifs_domain(ssh_client, vserver):
    """Fetches the CIFS domain for the specified SVM (vserver)."""
    try:
        command = f"cifs show -vserver {vserver} -fields domain"
        result = execute_ssh_command(ssh_client, command)

        # Split the result into lines
        lines = result.splitlines()

        # Skip the header and check for the actual domain data
        for line in lines:
            if vserver in line:
                columns = line.split()
                if len(columns) >= 2:
                    domain = columns[1].lower()  # Extract the domain and convert to lowercase
                    if "." in domain:  # Simple check to ensure it's a valid domain
                        return domain

        print(f"Domain not found in command output.")
        return None

    except Exception as e:
        print(f"Error fetching CIFS domain: {e}")
        return None

def modify_security_style_with_cli(ssh_client, vserver, volume_name, security_style):
    """Modifies the security style of a volume using the ONTAP CLI."""
    try:
        if security_style != "unix":
            command = (
                f"vol modify -vserver {vserver} -volume {volume_name} -security-style {security_style}"
            )
            execute_ssh_command(ssh_client, command)
            print(f"\nVolume '{volume_name}' security style set to '{security_style}'.\n")
        else:
            print(f"\nVolume '{volume_name}' is set to the default security style 'unix'.\n")
    except Exception as e:
        print(f"Error modifying security style for volume '{volume_name}': {e}\n")

def modify_snapshot_space_with_cli(ssh_client, vserver, volume_name, percent_snapshot):
    """Modifies the snapshot space of a volume using the ONTAP CLI."""
    try:
        command = (
            f"vol modify -vserver {vserver} -volume {volume_name} -percent-snapshot-space {percent_snapshot}"
        )
        execute_ssh_command(ssh_client, command)
    except Exception as e:
        print(f"Error modifying snapshot space for volume '{volume_name}': {e}\n")

def mount_volume_to_junction(ssh_client, vserver, volume_name, junction_path):
    """Mounts the volume to the specified junction path using the ONTAP CLI."""
    try:
        command = (
            f"volume mount -vserver {vserver} -volume {volume_name} -junction-path {junction_path}"
        )
        execute_ssh_command(ssh_client, command)
        print(f"\nVolume '{volume_name}' mounted at '{junction_path}'.\n")
    except Exception as e:
        print(f"Error mounting volume '{volume_name}' to '{junction_path}': {e}\n")

def create_cifs_share(ssh_client, vserver, share_name, junction_path):
    """Creates a CIFS share using the ONTAP CLI."""
    try:
        command = (
            f"cifs share create -vserver {vserver} -share-name {share_name} -path {junction_path} "
            "-share-properties oplocks,browsable,changenotify,show-previous-versions "
            "-symlink-properties symlinks"
        )
        execute_ssh_command(ssh_client, command)
        print(f"\nCIFS share '{share_name}' created at path '{junction_path}'.\n")
    except Exception as e:
        print(f"Error creating CIFS share '{share_name}': {e}\n")

def assign_export_policy_to_volume(ssh_client, vserver, volume_name, export_policy):
    """Assigns the export policy to the volume using the ONTAP CLI."""
    try:
        command = (
            f"vol modify -vserver {vserver} -volume {volume_name} -policy {export_policy}"
        )
        execute_ssh_command(ssh_client, command)
        print(f"\nExport policy '{export_policy}' assigned to volume '{volume_name}'.\n")
    except Exception as e:
        print(f"Error assigning export policy '{export_policy}' to volume '{volume_name}': {e}\n")

def create_export_policy_rule_with_ssh(ssh_client, vserver, policy_name, clientmatch, protocol, rorule, rwrule, rule_index, rule_details):
    """Creates an export policy rule using the ONTAP CLI."""
    try:
        command = (
            f"export-policy rule create -vserver {vserver} -policyname {policy_name} "
            f"-clientmatch {clientmatch} -protocol {protocol} -rorule {rorule} -rwrule {rwrule} "
            "-allow-suid true -allow-dev true -superuser sys"
        )
        result = execute_ssh_command(ssh_client, command)
        if result:
            print(f"\nExport policy rule {rule_index} created for policy '{policy_name}'.\n")
            rule_details.append(
                {
                    "rule_index": rule_index,
                    "clientmatch": clientmatch.split(','),  # Split the IPs into a list
                    "protocol": protocol,
                    "rorule": rorule,
                    "rwrule": rwrule
                }
            )
    except Exception as e:
        print(f"Error creating export policy rule {rule_index}: {e}\n")

def execute_ssh_command(ssh_client, command):
    """Executes a command over an existing SSH connection."""
    try:
        stdin, stdout, stderr = ssh_client.exec_command(command)
        stdout_str = stdout.read().decode()
        stderr_str = stderr.read().decode()
        
        if stderr_str:
            print(f"Error executing command: {stderr_str}\n")
        else:
            return stdout_str
    except Exception as e:
        print(f"Error executing command: {e}\n")

def create_volume(cluster, vserver, volume_name, aggregate, size, state, volume_type, snapshot_policy, foreground, junction_path, comment, username, password):
    """Creates a new volume on the specified SVM using the NetApp ONTAP REST API."""
    try:
        with HostConnection(
            host=cluster,
            username=username,
            password=password,
            verify=False
        ):
            # Explicitly handle the snapshot policy
            if snapshot_policy == "none":
                snapshot_policy_value = {"name": "none"}
            else:
                snapshot_policy_value = {"name": snapshot_policy}

            volume = Volume(
                name=volume_name,
                svm={"name": vserver},
                aggregates=[{"name": aggregate}],
                size=size,
                state=state,
                type=volume_type,
                snapshot_policy=snapshot_policy_value,  # Correctly pass snapshot policy
                foreground=foreground,
                junction_path=junction_path,
                tiering={"policy": "none"},  # Set tiering policy to 'none'
                comment=comment  # Add comment to the volume creation
            )

            volume.post()
            print(f"\nVolume '{volume_name}' created successfully.\n")  # Simplified output
            return True

    except NetAppRestError as e:
        print(f"Error creating volume: {e}\n")
        return False

def create_export_policy_with_api(cluster, vserver, policy_name, username, password):
    """Creates an export policy using the ONTAP REST API."""
    try:
        with HostConnection(
            host=cluster,
            username=username,
            password=password,
            verify=False
        ):
            export_policy = ExportPolicy(
                name=policy_name,
                svm={"name": vserver}
            )
            export_policy.post()
            print(f"\nExport policy '{policy_name}' created successfully.\n")
    except NetAppRestError as e:
        print(f"Error creating export policy via API: {e}\n")

def display_export_policy_rules(rule_details):
    """Displays the export policy rules in a tabular format with IPs on separate lines."""
    print("\nExport Policy Rules:\n")
    print(f"{'Rule Index':<12} {'Clientmatch':<30} {'Protocol':<10} {'RORule':<10} {'RWRule':<10}")
    print("-" * 75)
    for rule in rule_details:
        # The first line of the rule
        print(f"{rule['rule_index']:<12} {rule['clientmatch'][0]:<30} {rule['protocol']:<10} {rule['rorule']:<10} {rule['rwrule']:<10}")
        # Additional lines for the rest of the IPs in clientmatch
        for client_ip in rule['clientmatch'][1:]:
            print(f"{'':<12} {client_ip:<30}")
    print("\n")

def confirm_details(details):
    """Displays details for confirmation and asks for user confirmation"""
    print("\nPlease confirm the following details:")
    for key, value in details.items():
        print(f"{key}: {value}")
    confirm = input("\nAre these details correct? (yes/no): ").lower()
    return confirm == 'yes'

def display_vservers_in_columns(vservers, svm_tags):
    """Displays vservers in three columns if more than 20 SVMs are available."""
    if len(vservers) > 20:
        # Calculate the number of rows required to display the vservers in 3 columns
        rows = math.ceil(len(vservers) / 3)
        columns = [[], [], []]  # Prepare three columns

        # Distribute vservers into the columns and tag from JSON
        for i, vserver in enumerate(vservers):
            # Add SVM tag if available
            for tag, svms in svm_tags.items():
                if vserver.name in svms:
                    vserver_name = f"{vserver.name} ({tag})"
                    break
            else:
                vserver_name = vserver.name

            columns[i % 3].append(f"{i+1}. {vserver_name}")

        # Print the vservers in three columns
        print("\nAvailable Vservers (SVMs):")
        for row in range(rows):
            col1 = columns[0][row] if row < len(columns[0]) else ""
            col2 = columns[1][row] if row < len(columns[1]) else ""
            col3 = columns[2][row] if row < len(columns[2]) else ""
            print(f"{col1:<30} {col2:<30} {col3:<30}")

    else:
        # If there are 20 or fewer SVMs, display them in a single column
        print("\nAvailable Vservers (SVMs):")
        for idx, vserver in enumerate(vservers, start=1):
            # Add SVM tag if available
            for tag, svms in svm_tags.items():
                if vserver.name in svms:
                    vserver_name = f"{vserver.name} ({tag})"
                    break
            else:
                vserver_name = vserver.name

            print(f"{idx}. {vserver_name}")

def display_snapshot_policies_in_columns(snapshot_policies, cluster_name):
    """Displays snapshot policies in three columns if more than 20 are available,
    and includes the associated SVM for each policy."""
    # Extract the cluster name without the domain
    cluster_name_cleaned = cluster_name.split('.')[0]

    if len(snapshot_policies) > 20:
        # Calculate the number of rows required to display the snapshot policies in 3 columns
        rows = math.ceil(len(snapshot_policies) / 3)
        columns = [[], [], []]  # Prepare three columns

        # Distribute snapshot policies into the columns
        for i, policy in enumerate(snapshot_policies):
            policy.get()  # Fetch full details for each snapshot policy, including the SVM

            # Check if the svm field is missing or belongs to the cluster
            if hasattr(policy, 'svm') and hasattr(policy.svm, 'name'):
                svm_name = policy.svm.name
            else:
                svm_name = cluster_name_cleaned  # Use the cleaned cluster name if no SVM is attached

            columns[i % 3].append(f"{i+1}. {policy.name} ({svm_name})")

        # Print the snapshot policies in three columns
        print("\nAvailable Snapshot Policies:")
        for row in range(rows):
            col1 = columns[0][row] if row < len(columns[0]) else ""
            col2 = columns[1][row] if row < len(columns[1]) else ""
            col3 = columns[2][row] if row < len(columns[2]) else ""
            print(f"{col1:<30} {col2:<30} {col3:<30}")

    else:
        # If there are 20 or fewer snapshot policies, display them in a single column
        print("\nAvailable Snapshot Policies:")
        for idx, policy in enumerate(snapshot_policies, start=1):
            policy.get()  # Fetch full details for each snapshot policy, including the SVM

            # Check if the svm field is missing or belongs to the cluster
            if hasattr(policy, 'svm') and hasattr(policy.svm, 'name'):
                svm_name = policy.svm.name
            else:
                svm_name = cluster_name_cleaned  # Use the cleaned cluster name if no SVM is attached

            print(f"{idx}. {policy.name} ({svm_name})")

def load_svm_tags():
    """Loads the JSON file containing SVM tags."""
    try:
        with open("svm_mappings.json", "r") as json_file:
            svm_tags = json.load(json_file)
            return svm_tags
    except FileNotFoundError:
        print("SVM tagging JSON file not found.")
        return {}

if __name__ == "__main__":
    # Prompt for cluster, username, and password
    cluster = input("Enter the ONTAP cluster IP or hostname: ")
    username = input("Enter your NetApp username: ")
    password = getpass.getpass("Enter your NetApp password: ")

    # Load the SVM tagging JSON file
    svm_tags = load_svm_tags()

    # Set up an SSH client using paramiko
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Connect to the ONTAP cluster via SSH
        ssh_client.connect(cluster, username=username, password=password)

        # Fetch the list of aggregates
        aggregates = get_aggregates(cluster, username, password)

        if not aggregates:
            print("No aggregates found or error occurred while fetching aggregates.\n")
        else:
            # Display the list of aggregates to the user, along with available space and size
            print("\nAvailable Aggregates:")
            for idx, aggregate in enumerate(aggregates, start=1):
                aggregate_data = aggregate.to_dict()
                block_storage = aggregate_data.get('space', {}).get('block_storage', {})
                size_bytes = block_storage.get('size')
                available_size_bytes = block_storage.get('available')

                if size_bytes and available_size_bytes:
                    size_gb = format_size(size_bytes)
                    available_gb = format_size(available_size_bytes)
                    available_percent = (available_size_bytes / size_bytes) * 100
                    print(f"{idx}. {aggregate.name} - Size: {size_gb:.2f} GB, Available: {available_gb:.2f} GB ({available_percent:.2f}%)")
                else:
                    print(f"{idx}. {aggregate.name} - Size/Available space information unavailable")

            # Prompt the user to select an aggregate by number
            aggregate_choice = int(input("\nSelect an aggregate by number: ")) - 1

            if 0 <= aggregate_choice < len(aggregates):
                aggregate = aggregates[aggregate_choice].name
            else:
                print("Invalid selection.\n")
                exit(1)

            # Fetch the list of vservers (SVMs)
            vservers = get_vservers(cluster, username, password)

            if not vservers:
                print("No vservers found or error occurred while fetching vservers.\n")
                exit(1)
            else:
                # Display vservers in multiple columns if there are more than 20 SVMs
                display_vservers_in_columns(vservers, svm_tags)

                # Prompt the user to select a vserver by number
                vserver_choice = int(input("\nSelect a vserver by number: ")) - 1

                if 0 <= vserver_choice < len(vservers):
                    vserver = vservers[vserver_choice].name
                else:
                    print("Invalid selection.\n")
                    exit(1)

            # Fetch and display available snapshot policies
            snapshot_policies = get_snapshot_policies(cluster, username, password)
            if not snapshot_policies:
                print("No snapshot policies found or error occurred while fetching snapshot policies.\n")
            else:
                # Display snapshot policies in multiple columns if there are more than 20 policies
                display_snapshot_policies_in_columns(snapshot_policies, cluster)

                # Prompt the user to select a snapshot policy by number
                snapshot_policy_choice = int(input("\nSelect a snapshot policy by number: ")) - 1

                if 0 <= snapshot_policy_choice < len(snapshot_policies):
                    snapshot_policy = snapshot_policies[snapshot_policy_choice].name
                else:
                    snapshot_policy = "none"

            # Continue with other prompts
            volume_name = input("\nEnter the volume name: ")

            # Prompt for volume size
            size = input("Enter the size of the volume (e.g., 1TB, 500GB): ")

            # Format the volume name with the SVM
            formatted_volume_name = f"{vserver}_{volume_name}"

            state = "online"
            volume_type = "rw"
            foreground = True

            # Skip the junction path confirmation but still set it
            junction_path = f"/{formatted_volume_name}"

            # Only allow 'unix' and 'ntfs' for security style selection
            while True:
                security_style = input("Enter the security style (unix, ntfs): ").lower()
                if security_style in ['unix', 'ntfs']:
                    break
                else:
                    print("Invalid security style. Please enter 'unix' or 'ntfs'.\n")

            # Skip asking about multi-protocol if 'unix' is selected
            multi_protocol = "no"
            cifs_share_name = None
            if security_style != 'unix':
                multi_protocol = input("Is this a multi-protocol volume? (yes/no): ").lower()
                # Ask for CIFS share name if NTFS is selected
                cifs_share_name = input("Enter the CIFS share name for the NTFS volume: ")

            percent_snapshot = int(input("Enter the percentage of space for snapshots: "))
            comment = input("Enter a comment for the volume: ")  # Prompt for comment

            # Summary of the inputs for confirmation
            details = {
                "Cluster": cluster,
                "Vserver": vserver,
                "Volume Name": formatted_volume_name,
                "Aggregate": aggregate,
                "Size": size,
                "State": state,
                "Security Style": security_style,
                "Volume Type": volume_type,
                "Foreground": foreground,
                "Snapshot Policy": snapshot_policy,
                "Junction Path": junction_path,
                "Snapshot Space Percentage": percent_snapshot,
                "Comment": comment,
                "Multi-Protocol": multi_protocol
            }
            if security_style == 'ntfs':
                details["CIFS Share Name"] = cifs_share_name

            # Confirm details before proceeding
            if not confirm_details(details):
                print("Process canceled by the user.")
                exit(0)

            print("\n")  # Add space before starting output

            # Create the volume
            volume_created = create_volume(
                cluster=cluster,
                vserver=vserver,
                volume_name=formatted_volume_name,
                aggregate=aggregate,
                size=size,
                state=state,
                volume_type=volume_type,
                snapshot_policy=snapshot_policy,
                foreground=foreground,
                junction_path=junction_path,
                comment=comment,
                username=username,
                password=password
            )

            # After the volume is created, modify the security style via CLI (if necessary)
            if volume_created:
                modify_security_style_with_cli(ssh_client, vserver, formatted_volume_name, security_style)

                # Modify snapshot space using CLI without showing a message
                modify_snapshot_space_with_cli(ssh_client, vserver, formatted_volume_name, percent_snapshot)

                # Mount the volume to the junction path
                mount_volume_to_junction(ssh_client, vserver, formatted_volume_name, junction_path)

                # For NTFS volumes, create CIFS share
                if security_style == 'ntfs' and cifs_share_name:
                    create_cifs_share(ssh_client, vserver, cifs_share_name, junction_path)

                    # Fetch CIFS domain dynamically
                    domain = get_cifs_domain(ssh_client, vserver)

                    # Multi-protocol message is delayed until after all operations
                    if domain and multi_protocol == 'no':
                        # Construct a customer-facing message for NTFS, non-multi-protocol volumes
                        customer_message = (
                            f"\nYour requested CIFS volume has been successfully provisioned.\n"
                            f"You can access the share at the following path:\n"
                            f"\\\\{vserver}.{domain}\\{cifs_share_name}\n\n"
                            f"Volume Details:\n"
                            f"- Total Capacity: {size}, with {percent_snapshot}% reserved for snapshots.\n"
                            f"- Snapshot Policy Applied: {snapshot_policy}\n\n"
                            f"By default, permission is set to Everyone, a sibling task will be created to EEE - RF - Directory Services "
                            f"to restrict permission once the NAS task is closed.\n"
                            f"Also, please be advised that we do not manage copying data. You may raise a request with the Windows team."
                        )
                        print(customer_message)

                # For UNIX volumes, create export policy and NFS rule
                if security_style == 'unix':
                    # Create export policy via API
                    create_export_policy_with_api(cluster, vserver, formatted_volume_name, username, password)
                    
                    # Create NFS rule for export policy via CLI
                    clientmatch_ips = input("Enter IPs for clientmatch (comma-separated): ")
                    rule_details = []
                    create_export_policy_rule_with_ssh(ssh_client, vserver, formatted_volume_name, clientmatch_ips, 'nfs', 'any', 'any', 1, rule_details)

                    # Display the export-policy rule details
                    display_export_policy_rules(rule_details)

                    # Fetch CIFS domain dynamically for NFS path
                    domain = get_cifs_domain(ssh_client, vserver)

                    # Customer-facing message for UNIX security style
                    customer_message_unix = (
                        f"\nYour requested NFS volume has been successfully provisioned.\n"
                        f"For NFS access, you can mount the volume using the following path:\n"
                        f"{vserver}.{domain}:/{formatted_volume_name}\n\n"
                        f"Volume Details:\n"
                        f"- Total Capacity: {size}, with {percent_snapshot}% reserved for snapshots.\n"
                        f"- Snapshot Policy Applied: {snapshot_policy}\n\n"
                        f"Your NFS export has the following IPs allowed for access:\n"
                    )

                    # Append the IPs line by line for NFS access
                    for ip in clientmatch_ips.split(','):
                        customer_message_unix += f"- {ip}\n"

                    customer_message_unix += "\nThank you for your request. If you require further assistance, feel free to reach out."

                    print(customer_message_unix)

                # Handle multi-protocol export policy creation if necessary
                if multi_protocol == 'yes':
                    # First create the export policy using API
                    create_export_policy_with_api(cluster, vserver, formatted_volume_name, username, password)
                    
                    # Assign the export policy to the volume
                    assign_export_policy_to_volume(ssh_client, vserver, formatted_volume_name, formatted_volume_name)

                    # Create "RULE INDEX 1" for CIFS via SSH
                    rule_details = []
                    create_export_policy_rule_with_ssh(ssh_client, vserver, formatted_volume_name, '0.0.0.0/0', 'cifs', 'any', 'any', 1, rule_details)

                    # Ask if the user wants to create "RULE INDEX 2" for NFS
                    create_rule_2 = input("\nDo you want to create RULE INDEX 2 for NFS? (yes/no): ").lower()
                    if create_rule_2 == 'yes':
                        clientmatch_ips = input("Enter IPs for clientmatch (comma-separated): ")
                        create_export_policy_rule_with_ssh(ssh_client, vserver, formatted_volume_name, clientmatch_ips, 'nfs', 'any', 'any', 2, rule_details)

                    # Display the export policy rules in a column format with multi-line clientmatch
                    display_export_policy_rules(rule_details)

                    # After export-policy rules, generate the customer-facing message
                    if domain:
                        customer_message = (
                            f"\nYour multi-protocol volume has been successfully provisioned.\n"
                            f"You can access the CIFS share at the following path:\n"
                            f"\\\\{vserver}.{domain}\\{cifs_share_name}\n\n"
                            f"For NFS access, you can mount the volume using the following path:\n"
                            f"{vserver}.{domain}:/{formatted_volume_name}\n\n"
                            f"Volume Details:\n"
                            f"- Total Capacity: {size}, with {percent_snapshot}% reserved for snapshots.\n"
                            f"- Snapshot Policy Applied: {snapshot_policy}\n\n"
                            f"Your NFS export has the following IPs allowed for access:\n"
                        )

                        # Append the IPs line by line for NFS access
                        for ip in clientmatch_ips.split(','):
                            customer_message += f"- {ip}\n"

                        customer_message += (
                            "\nBy default, permission is set to Everyone, a sibling task will be created to GROUP"
                            "to restrict permission once the NAS task is closed.\n"
                            "Also, please be advised that we do not manage copying data. You may raise a request with the Windows team."
                        )

                        print(customer_message)
                    else:
                        print("\nVolume created but could not retrieve the CIFS domain. Please verify manually.\n")

    finally:
        print("\nClosing SSH connection...\n")
        ssh_client.close()
