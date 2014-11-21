vcycle
======

Configuration to use with occi interface
----------------------------------------

- Be sure you have installed occi client on your machine.
- Create a new voms proxy.
- Edit /etc/vcycle.conf with following format:


[tenancy name_tenancy] 

tenancy_name = name 

url = occi_endpoint

proxy = proxy_file_dir

max_machines = max_number_of_machines_in_the_tenant

type = occi


[vmtype name_tenancy QUEUE_NAME]

ce_name = CE_NAME

max_machines = max_machines_to_execute_with_vcycle

backoff_seconds = seconds

fizzle_seconds = seconds

max_wallclock_seconds = seconds

image_name = id_of_the_image

flavor_name = name_of_the_flavor

x509dn = dn_to_use_to_execute_jobs

heartbeat_file = name_of_the_heartbeat_file

heartbeat_seconds = seconds_between_heartbeats

