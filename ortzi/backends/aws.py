import json
import os
import socket

import yaml
import boto3
from lithops.config import get_default_config_filename, load_yaml_config, \
    load_config
from lithops.constants import SA_CONFIG_FILE, SA_INSTALL_DIR, \
    SA_MASTER_DATA_FILE, SA_SETUP_LOG_FILE
from lithops.standalone.utils import MASTER_SERVICE_FILE, \
    MASTER_SERVICE_NAME, docker_login

from ortzi.utils import rands


AWS_CONFIG_FILE = ".aws/credentials"
AWS_LITHOPS_EQUIVALENCES = {
    "aws_access_key_id": "access_key_id",
    "aws_secret_access_key": "secret_access_key",
    "aws_session_token": "session_token"
}
AWS_CONTAINER_MEMORY_PERC = 0.5
AWS_LAMBDA_COST_MB = 0.00001667 / 1024
AWS_LAMBDA_REQ_COST = 0.2 / 10 ** 6
AWS_S3_PUT_COST = 0.005 / 1000
AWS_S3_GET_COST = 0.0004 / 1000

AWS_INSTANCES_COST = {
    "t2.xlarge": 0.1856
}


def set_lithops_config_aws(
    aws_config_path: str = None,
    lithops_config_path: str = None
):
    if lithops_config_path is None:
        config_filename = get_default_config_filename()
        if config_filename:
            lithops_config = load_yaml_config(config_filename)

    home_dir = os.path.expanduser("~")
    if aws_config_path is None:
        aws_config_path = os.path.join(home_dir, AWS_CONFIG_FILE)

    aws_config = {}
    with open(aws_config_path, 'r') as f:
        lines = f.readlines()[1:]  # Skip the first line with the profile
        for line in lines:
            key, value = line.strip().split('=', 1)
            aws_config[key] = value

    if "aws" not in lithops_config:
        lithops_config["aws"] = {}

    for aws_key, lithops_key in AWS_LITHOPS_EQUIVALENCES.items():
        if aws_key in aws_config:
            lithops_config["aws"][lithops_key] = aws_config[aws_key]

    with open(config_filename, 'w') as f:
        yaml.dump(lithops_config, f)


def launch_ec2_instance(
    ec2,
    instance_type: str = "t2.small",
    image_id: str = None,
    snapshot_id: str = None,
    key_id: str = None,
    subnet_id: str = None,
    security_group_id: str = None,

) -> str:

    lithops_config = load_config()

    if instance_type is None:
        instance_type = lithops_config["aws_ec2"]["worker_instance_type"]

    if image_id is None:
        image_id = lithops_config["aws_ec2"]["target_ami"]

    if snapshot_id is None:
        response = ec2.describe_images(ImageIds=[image_id])
        snapshot_id = response['Images'][0]['BlockDeviceMappings'][0]['Ebs']['SnapshotId']

    if key_id is None:
        key_id = lithops_config["aws_ec2"]["ssh_key_name"]

    if subnet_id is None:
        subnet_id = lithops_config["aws_ec2"]["subnet_id"]

    if security_group_id is None:
        security_group_id = lithops_config["aws_ec2"]["security_group_id"]

    instance_name = f"ortzi-worker-{rands(5)}"
    response = ec2.run_instances(
        ImageId=image_id,
        InstanceType=instance_type,
        KeyName=key_id,
        BlockDeviceMappings=[
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "Encrypted": False,
                    "DeleteOnTermination": True,
                    "SnapshotId": snapshot_id,
                    "VolumeSize": 30,
                    "VolumeType": "gp2",
                },
            }
        ],
        NetworkInterfaces=[
            {
                "SubnetId": subnet_id,
                "AssociatePublicIpAddress": True,
                "DeviceIndex": 0,
                "Groups": [security_group_id],
            }
        ],
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": instance_name}],
            }
        ],
        PrivateDnsNameOptions={
            "HostnameType": "ip-name",
            "EnableResourceNameDnsARecord": False,
            "EnableResourceNameDnsAAAARecord": False,
        },
        MinCount=1,
        MaxCount=1,
    )

    instance_id = response["Instances"][0]["InstanceId"]
    return instance_id, instance_name


def get_private_ip() -> str:
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


def get_ssh_key_filename() -> str:
    lithops_config = load_config()
    return lithops_config["aws_ec2"]["ssh_key_filename"]


def get_instance_private_ip(instance_id: str) -> str:
    ec2 = boto3.client('ec2')
    response = ec2.describe_instances(InstanceIds=[instance_id])
    private_ip = response['Reservations'][0]['Instances'][0]['PrivateIpAddress']
    return private_ip


def start_instance_by_id(instance_id: str):
    ec2 = boto3.client('ec2')
    ec2.start_instances(InstanceIds=[instance_id])
    return instance_id


def get_total_vcpus():
    return os.cpu_count()


def get_master_setup_script(config, vm_data):
    """
    Returns master VM installation script
    """
    script = docker_login(config)
    script += f"""
    setup_host(){{
    unzip -o /tmp/lithops_standalone.zip -d {SA_INSTALL_DIR};
    mv /tmp/lithops_standalone.zip {SA_INSTALL_DIR};
    echo '{json.dumps(vm_data)}' > {SA_MASTER_DATA_FILE};
    echo '{json.dumps(config)}' > {SA_CONFIG_FILE};
    }}
    setup_host >> {SA_SETUP_LOG_FILE} 2>&1;
    """
    script += f"""
    FILE_PATH="{SA_INSTALL_DIR}/lithops/standalone/utils.py"
    """
    script += r'''
    sed -i "s/cmd_start = 'docker run --rm --name lithops_worker '\$/cmd_start = f'docker run --rm --name lithops_worker --shm-size={config[\"shm_size\"]}M '/" $FILE_PATH
    '''
    script += f"""
    setup_service(){{
    echo '{MASTER_SERVICE_FILE}' > /etc/systemd/system/{MASTER_SERVICE_NAME};
    chmod 644 /etc/systemd/system/{MASTER_SERVICE_NAME};
    systemctl daemon-reload;
    systemctl stop {MASTER_SERVICE_NAME};
    systemctl enable {MASTER_SERVICE_NAME};
    systemctl start {MASTER_SERVICE_NAME};
    }}
    setup_service >> {SA_SETUP_LOG_FILE} 2>&1;
    USER_HOME=$(eval echo ~${{SUDO_USER}});
    generate_ssh_key(){{
    echo '    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null' >> /etc/ssh/ssh_config;
    ssh-keygen -f $USER_HOME/.ssh/lithops_id_rsa -t rsa -N '';
    chown ${{SUDO_USER}}:${{SUDO_USER}} $USER_HOME/.ssh/lithops_id_rsa*;
    cp $USER_HOME/.ssh/lithops_id_rsa $USER_HOME/.ssh/id_rsa
    cp $USER_HOME/.ssh/lithops_id_rsa.pub $USER_HOME/.ssh/id_rsa.pub
    cp $USER_HOME/.ssh/* /root/.ssh;
    echo '127.0.0.1 lithops-master' >> /etc/hosts;
    cat $USER_HOME/.ssh/id_rsa.pub >> $USER_HOME/.ssh/authorized_keys;
    }}
    test -f $USER_HOME/.ssh/lithops_id_rsa || generate_ssh_key >> {SA_SETUP_LOG_FILE} 2>&1;
    echo 'tail -f -n 100 /tmp/lithops-*/master-service.log'>>  $USER_HOME/.bash_history
    """
    return script


def patch_lithops_aws():

    import lithops.standalone.utils
    import lithops.standalone.standalone

    lithops.standalone.utils.get_master_setup_script = get_master_setup_script
    lithops.standalone.standalone.get_master_setup_script = get_master_setup_script


def get_ec2_memory_size(
    ec2_client,
    instance_id,
    instance_type=None
):

    if instance_type is None:
        # Get the instance type for the given instance ID
        response = ec2_client.describe_instances(InstanceIds=[instance_id])
        instance_type = response["Reservations"][0]["Instances"][0]["InstanceType"]

    # Get the memory size for the instance type
    response = ec2_client.describe_instance_types(InstanceTypes=[instance_type])
    memory_mib = response["InstanceTypes"][0]["MemoryInfo"]["SizeInMiB"]

    return memory_mib


def get_shared_memory_size(ec2_client, instance_id, instance_type=None):

    memory_mib = get_ec2_memory_size(ec2_client, instance_id, instance_type)
    return int(memory_mib * AWS_CONTAINER_MEMORY_PERC)


def setup_instance(ec2, instance_id, instance_type: None):

    print("Setting up AWS configuration")
    set_lithops_config_aws()

    if instance_id is None or instance_id == "":
        instance_id, instance_name = \
            launch_ec2_instance(ec2, instance_type=instance_type)
    else:
        instance_id = instance_id
        start_instance_by_id(instance_id)

    print("Waiting for instance to be running...")
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id])

    executor_ip = get_instance_private_ip(instance_id)

    # Necessary to do shared memory exchanges within the
    # container.
    patch_lithops_aws()
    return instance_id, executor_ip


def cmd_remove_s3(bucket: str):
    return f"aws s3 rm s3://{bucket}/ --recursive"