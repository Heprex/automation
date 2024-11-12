
# NetApp SnapMirror Disaster Recover (DR)

This repository provides a script to automate SnapMirror management across NetApp clusters, focusing on tasks such as updating replication links, recovery operations, and restoration actions. The script interfaces with clusters over SSH, displaying detailed SnapMirror status tables for both active PROD-to-DR and DR-to-PROD replication paths.

## Features

- Displays SnapMirror replication status in detailed tables for both PROD-to-DR and DR-to-PROD links.
- Allows operations like update, quiesce, break, resync, recovery, and restoration on SnapMirror relationships.
- Supports parallel data fetching for improved efficiency when loading SnapMirror details.
- Logs actions with user details and timestamp to a shared network file, maintaining a history of operations.


## Prerequisites

- **Python version**: 3.6 or higher
- **Required Python packages**:
  - `paramiko`
  - `pytz`
  - `tzlocal`
  - `getpass4`
  - `termcolor`
  - `colorama`
  - `PyYAML`
  - `tabulate`

Install the required packages with:
```bash
pip3 install paramiko tzlocal termcolor colorama PyYAML tabulate getpass4 pytz
```

## Getting Started

### Cloning the Repository

To start using the SnapMirror Management Automation script, clone the repository:

```bash
git clone git@github.service.anz:Storage-Play-Area/practice-automation.git
```

Navigate to the repository folder:

```bash
cd practice-automation/netapp_python/functioning/dr/
```

Before each use, update to the latest version by pulling changes:

```bash
git pull
```

### 1. Prepare the Input File

Create a YAML input file named `snapmirror_input.yaml` to define applications, clusters, vservers, and volumes. Below is an example structure:

```yaml
- app_name: App1
  dr_cluster: dr_cluster_address
  prod_cluster: prod_cluster_address
  dr_vserver: dr_vserver_name
  prod_vserver: prod_vserver_name
  details: "enter details here"
  volume_names:
    - volume_name: volume1
      share_name: share_name1
    - volume_name: volume2
      qtrees:
        - qtree_name: qtree1
          share_name: qtree1_share
```

### 2. Running the Script

Ensure `snapmirror_input.yaml` is in the same directory as the script. Then, run the script:

```bash
python3 nas-dr.py
```

Upon starting, you will be prompted to enter your username and password for SSH connections. The script then loads and displays SnapMirror tables, allowing you to select an application and specify an action.

### 3. Available Actions

The following actions are supported for each application:

- `show details`: View application details and SnapMirror information.
- `update`: Update SnapMirror replication based on active direction (PROD-to-DR or DR-to-PROD).
- `quiesce`: Temporarily pause SnapMirror updates.
- `break`: Break the SnapMirror relationship for a volume.
- `resync`: Resync SnapMirror from source to destination.
- `recovery`: Perform recovery actions across all volumes.
- `recovery-extended`: Reverse replication post-test verification (TVT) process.
- `restoration-extended`: Extended restoration of replication and shares.
- `restoration-flip-flop`: Restore original direction without reverse replication.
- `restoration-post-tvt`: Re-establish replication post-test verification.

### DR Typical Workflow

- **Flip-Flop or No Reverse Replication Needed**:
  - `recovery` -> `restoration-flip-flop` -> `restoration-post-tvt`

- **Extended or DR data is needed**:
  - `recovery` -> `recovery-extended` -> `restoration-extended` -> `restoration-post-tvt`


### Logging

Confirmed actions are logged in a shared file located at `RECENT_ACTIONS_FILE`, capturing:

- **Action**: Name of the action performed.
- **User**: Username of the person who executed the action.
- **Timestamp**: Date and time of action execution in `DD-MMM-YYYY hh:mm:ss AM/PM` format.

The log file maintains a history of actions for auditing purposes.

### Error Handling

The script handles errors during SSH connection or SnapMirror command execution. Errors are logged, and user-friendly messages are displayed to aid in troubleshooting.

## Important Notes

- The script detects the local timezone automatically for accurate timestamps, supporting users in different timezones.
- Actions are only logged if confirmed by the user. Invalid actions are not recorded.
