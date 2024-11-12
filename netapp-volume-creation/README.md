
# NetApp Volume Creation

This script automates the creation of a volume on a NetApp ONTAP cluster, including setting up a CIFS or NFS export depending on the volume's security style and configuration. It leverages the NetApp ONTAP REST API for volume and export policy management, and the ONTAP CLI for additional configurations through SSH.

## Features

- Lists aggregates and SVMs on the specified ONTAP cluster.
- Creates volumes with specified parameters, including snapshot policies, security styles, and junction paths.
- Supports CIFS (SMB) shares and NFS exports for multi-protocol volumes.
- Automatically configures snapshot space, security style, and export policy for volumes.
- Provides user-friendly output and customer-facing messages upon successful volume creation.

## Requirements

- Python 3.x
- `paramiko` for SSH operations
- `netapp_ontap` for REST API access to the ONTAP cluster
- Access to the ONTAP cluster using valid credentials

## Setup

1. Install required Python packages:
   ```bash
   pip install paramiko netapp-ontap
   ```

2. Configure access to the ONTAP cluster and ensure permissions for volume and export management.

3. Ensure the script has access to the `svm_mappings.json` file containing SVM tags in the following format:
   ```json
   {
       "MTZ": ["svm1", "svm2"],
       "HZT": ["svm3", "svm4"],
       "VHTZ": ["svm5", "svm6"]
   }
   ```

## Usage

Run the script from a terminal with Python 3:
```bash
python new_volume-non-rep.py
```

### Prompted Parameters

The script will prompt you to enter the following information:

1. **Cluster IP or Hostname**: The IP address or hostname of the ONTAP cluster.
2. **Username and Password**: Your ONTAP credentials.
3. **Aggregate Selection**: A list of available aggregates is displayed for selection.
4. **Vserver (SVM) Selection**: A list of available SVMs is displayed, with SVMs tagged according to `svm_mappings.json`.
5. **Snapshot Policy**: Choose a snapshot policy from the available options, excluding any with `-DR` in the name.
6. **Volume Name**: Enter a name for the volume. This will be formatted as `SVM_VolumeName`.
7. **Volume Size**: Specify the volume size (e.g., `500GB`, `1TB`).
8. **Security Style**: Choose between `unix` and `ntfs` security styles.
9. **CIFS Share Name** (if applicable): For NTFS volumes, enter a CIFS share name.
10. **Snapshot Space Percentage**: Specify the percentage of the volume reserved for snapshots.
11. **Comment**: Enter any comments about the volume.

### Default Parameters

- **Volume State**: `online`
- **Volume Type**: `rw`
- **Foreground**: `True`

### Confirmation

A summary of the selected parameters is displayed for confirmation before proceeding with volume creation.

## Customer-Facing Messages

Upon successful volume creation, a message with access details is displayed based on the security style and configuration:

1. **NTFS Security Style (CIFS share)**:
   ```
   Your requested CIFS volume has been successfully provisioned.
   You can access the share at the following path:
   \\{vserver}.{domain}\{share_name}

   Volume Details:
   - Total Capacity: {size}, with {percent_snapshot}% reserved for snapshots.
   - Snapshot Policy Applied: {snapshot_policy}
   ```

2. **UNIX Security Style (NFS export)**:
   ```
   Your requested NFS volume has been successfully provisioned.
   For NFS access, you can mount the volume using the following path:
   {vserver}.{domain}:/{volume_name}

   Volume Details:
   - Total Capacity: {size}, with {percent_snapshot}% reserved for snapshots.
   - Snapshot Policy Applied: {snapshot_policy}
   ```

3. **Multi-Protocol Volume (CIFS & NFS)**:
   ```
   Your multi-protocol volume has been successfully provisioned.
   CIFS path: \\{vserver}.{domain}\{share_name}
   NFS mount path: {vserver}.{domain}:/{volume_name}
   ```

## Closing

The script closes the SSH connection to the ONTAP cluster upon completion.
