import os
import boto3
import json_operations 
import json

# loading data from JSON 
config_data = json_operations.loadJsonData("./configs/config.json")
key_path = config_data["key_path"]
key_name = config_data["key_name"]
ami_id = config_data["ami_id"]
instance_type = config_data["instance_type"]
region_name = config_data["region_name"]
ec2_json_data_path = config_data["ec2_data_path"]
ec2_data = json_operations.loadJsonData(ec2_json_data_path)

ec2_client = boto3.client("ec2", region_name=region_name)

# create key_pair for EC2 instance
def create_key_pair():
    if not os.path.exists(key_path):
        key_pair = ec2_client.create_key_pair(KeyName=key_name)
        private_key = key_pair["KeyMaterial"]
        # writing the key & changing it's permission to 400
        with os.fdopen(os.open(key_path, os.O_WRONLY | os.O_CREAT, 0o400), "w+") as handle:
            handle.write(private_key)
#create_key_pair()

# create EC2 Instance
def create_instance():
    instances = ec2_client.run_instances(
        ImageId = ami_id,
        MinCount = 1,
        MaxCount = 1,
        InstanceType = instance_type,
        KeyName =  key_name
    )
    instance_id = instances["Instances"][0]["InstanceId"]
    print(instance_id)
    if "ec2_instance_ids" in ec2_data:
        ec2_data["ec2_instance_ids"].append(instance_id)
    else:
        ec2_data["ec2_instance_ids"] = [instance_id]
create_instance()

# for creating multiple instances
for x in range(5):
   create_instance()

# getting the public IP of instance
def get_public_ip(instance_id):
    reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).get("Reservations")

    for reservation in reservations:
        for instance in reservation['Instances']:
            print(instance.get("PublicIpAddress"))
get_public_ip("i-0893f346bf16424ba")

# getting the private IP of instance
def get_public_ip(instance_id):
    reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).get("Reservations")

    for reservation in reservations:
        for instance in reservation['Instances']:
            print(instance.get("PrivateIpAddress"))
get_public_ip("i-0893f346bf16424ba")

# getting the VPC ID of instance
def get_private_ip(instance_id):
    reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).get("Reservations")

    for reservation in reservations:
        for instance in reservation['Instances']:
            print(instance.get("VpcId"))
get_private_ip("i-0893f346bf16424ba")

# getting the Subnet ID of instance
def get_subnet_id(instance_id):
    reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).get("Reservations")

    for reservation in reservations:
        for instance in reservation['Instances']:
            print(instance.get("SubnetId"))
get_subnet_id("i-0893f346bf16424ba")

# getting all running instances
def get_running_instances():
    reservations = ec2_client.describe_instances(Filters=[
        {
            "Name": "instance-state-name",
            "Values": ["running"],
        }
    ]).get("Reservations")
        
    for reservation in reservations:
        for instance in reservation["Instances"]:
            instance_id = instance["InstanceId"]
            instance_type = instance["InstanceType"]
            public_ip = instance["PublicIpAddress"]
            private_ip = instance["PrivateIpAddress"]
            print(f"{instance_id}, {instance_type}, {public_ip}, {private_ip}")
            
get_running_instances()

# reboot instance
def reboot_instance(instance_id):
    response = ec2_client.reboot_instances(InstanceIds=[instance_id])
    print(response)
reboot_instance("i-0893f346bf16424ba")

# stop instance
def reboot_instance(instance_id):
    response = ec2_client.stop_instances(InstanceIds=[instance_id])
    print(response)
reboot_instance("i-0893f346bf16424ba")

# start instance
def start_instance(instance_id):
    response = ec2_client.start_instances(InstanceIds=[instance_id])
    print(response)
start_instance("i-0893f346bf16424ba")

# terminate instance
def terminate_instance(instance_id):
    response = ec2_client.terminate_instances(InstanceIds=[instance_id])
    print(response)
    ec2_data["ec2_instance_ids"].remove(instance_id)
terminate_instance("i-0893f346bf16424ba")

# terminate multiple instances
def terminate_instances(instance_ids):
    response = ec2_client.terminate_instances(InstanceIds=instance_ids)
    print(response)
    final_instances_list = list(filter(lambda item: (item not in instance_ids) , ec2_data["ec2_instance_ids"]))
    ec2_data["ec2_instance_ids"] = final_instances_list
    print(json.dumps(ec2_data["ec2_instance_ids"]))
terminate_instances(ec2_data["ec2_instance_ids"])
