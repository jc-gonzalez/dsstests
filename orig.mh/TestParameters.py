# FileParameters.py
# Main parameters used by the TestDSS.py script 
seed = "2103473047672345432345676953214543219"     # Integer seed
base_file_location = "test-data/"    # Absolute or relative path to where files are created, include trailing slash, must be writable to the user running the script.  Ideally this is set to be the same as your cluster workspace.
dss_client_location = "/opt/euclid-ial/core/bin/dssserver_client.py"  # Absolute path to the DSS client script
dss_delete_location = "/opt/euclid-ial/core/bin/ds.sh"  # Absolute path to the DSS delete script
store_dss_url = "hostname:port"  # URL of DSS server where files should be stored (either your local DSS or a remote one)
dss_username = "USERNAME"   # EAS username to run commands
dss_password = "PASSWORD"   # EAS password for the user
sdc_name = "SDC-NAME"      #String