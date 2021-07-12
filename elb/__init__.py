import boto3
from botocore import exceptions

PREFIX="cache-elb"

elb = boto3.client('elbv2', region_name='us-east-1')
ec2 = boto3.client('ec2', region_name='us-east-1')

class ELB(object):
    """
        Class that operates all ELB common methods (including the creation of the ELB if not exist)
    """

    def __init__(self, instance_id):
        self.instance_id = instance_id
        print('Initiating ELB handler for instance id - ' + str(instance_id))

    def get_instance_ip_by_id(self, instance_id):
        instances = ec2.describe_instances(
        Filters=[
            {
                'instance-id': 'string',
                'Values': [
                    instance_id,
                ]
            },
        ],
        )

        print(instances)

        for instance in instances:
            if instance.instance_id == instance_id:
                return instance.public_ip_address
        return None

    def init_security_groups(self, vpc_id):
        try:
            response = ec2.describe_security_groups(GroupNames=[PREFIX+"elb-access"])
            elb_access = response["SecurityGroups"][0]
            response = ec2.describe_security_groups(GroupNames=[PREFIX+"instance-access"])
            instance_access = response["SecurityGroups"][0]
            return {
                "elb-access": elb_access["GroupId"], 
                "instance-access": instance_access["GroupId"], 
            }
        except exceptions.ClientError as e:
            if e.response['Error']['Code'] != 'InvalidGroup.NotFound':
                raise e

        vpc = ec2.describe_vpcs(VpcIds=[vpc_id])
        #cidr_block = vpc["Vpcs"][0]["CidrBlock"]

        elb = ec2.create_security_group(
            Description="ELB External Access",
            GroupName=PREFIX+"elb-access",
            VpcId=vpc_id
        )
        elb_sg = boto3.resource('ec2').SecurityGroup(elb["GroupId"])
        elb_sg.authorize_ingress(
            CidrIp="0.0.0.0/0",
            FromPort=80,
            ToPort=80,
            IpProtocol="TCP",
        )
        
        instances = ec2.create_security_group(
            Description="ELB Access to instances",
            GroupName=PREFIX+"instance-access",
            VpcId=vpc_id
        )
        instance_sg = boto3.resource('ec2').SecurityGroup(instances["GroupId"])
        instance_sg.authorize_ingress(
            CidrIp="0.0.0.0/0",
            FromPort=8080,
            ToPort=8080,
            IpProtocol="TCP",
        )
        return {
            "elb-access": elb["GroupId"], 
            "instance-access": instances["GroupId"]
        }
        


    def get_default_subnets(self):
        response = ec2.describe_subnets(
            Filters=[{"Name": "default-for-az", "Values": ["true"]}]
        )
        subnetIds = [s["SubnetId"] for s in response["Subnets"]]
        return subnetIds

    # creates the ELB as well as the target group
    # that it will distribute the requests to
    def ensure_elb_setup_created(self):
        response = None
        try:
            response = elb.describe_load_balancers(Names=[PREFIX])
        except exceptions.ClientError as e:
            if e.response['Error']['Code'] != 'LoadBalancerNotFound':
                raise e
            subnets = self.get_default_subnets()
            response= elb.create_load_balancer(
                Name=PREFIX,
                Scheme='internet-facing',
                IpAddressType='ipv4',
                Subnets=subnets,
            )
        elb_arn = response["LoadBalancers"][0]["LoadBalancerArn"]
        vpc_id = response["LoadBalancers"][0]["VpcId"]
        results = self.init_security_groups(vpc_id)
        elb.set_security_groups(
            LoadBalancerArn=elb_arn,
            SecurityGroups=[results["elb-access"]]
        )
        target_group=None
        try:
            target_group = elb.describe_target_groups(
                Names=[PREFIX +"-tg"],
            )
        except exceptions.ClientError as e:
            if e.response['Error']['Code'] != 'TargetGroupNotFound':
                raise e
            target_group = elb.create_target_group(
                Name=PREFIX +"-tg",
                Protocol="HTTP",
                Port=80,
                VpcId=vpc_id,
                HealthCheckProtocol="HTTP",
                HealthCheckPort="8080",
                HealthCheckPath="/health-check",
                TargetType="instance",
            )
        target_group_arn= target_group["TargetGroups"][0]["TargetGroupArn"]
        listeners = elb.describe_listeners(LoadBalancerArn=elb_arn)
        if len(listeners["Listeners"]) == 0:
            elb.create_listener(
                LoadBalancerArn=elb_arn,
                Protocol="HTTP",
                Port=80,
                DefaultActions=[
                    {
                        "Type": "forward",
                        "TargetGroupArn": target_group_arn,
                        "Order": 100
                    }
                ]
            )
        return results 

    def register_instance_in_elb(self, instance_id):
        results = self.ensure_elb_setup_created()
        target_group = elb.describe_target_groups(
            Names=[PREFIX + "-tg"],
        )

        instance = boto3.resource('ec2').Instance(instance_id)
        sgs = [sg["GroupId"] for sg in instance.security_groups]
        sgs.append(results["instance-access"])
        instance.modify_attribute(
            Groups=sgs
        )
        target_group_arn = target_group["TargetGroups"][0]["TargetGroupArn"]
        elb.register_targets(
            TargetGroupArn=target_group_arn,
            Targets=[{
                "Id": instance_id,
                "Port": 8080
            }]
        )

    def get_targets_status(self):
        target_group = elb.describe_target_groups(
            Names=[PREFIX+"-tg"],
        )
        target_group_arn= target_group["TargetGroups"][0]["TargetGroupArn"]
        health = elb.describe_target_health(TargetGroupArn=target_group_arn)
        healthy=[]
        sick={}
        for target in health["TargetHealthDescriptions"]:
            if target["TargetHealth"]["State"] == "unhealthy":
                sick[target["Target"]["Id"]] = target["TargetHealth"]["Description"]
            else:
                healthy.append(target["Target"]["Id"])
        return healthy, sick