import boto3
import json
import csv
import datetime
import pprint
import time
import subprocess
import io
import argparse

from fabric import Connection, Config
import fabric as fabric
import tempfile
import socket
import os

#awsAccessId_default='#####################'
#awsAccessKey_default='#####################'
keyFile_default = 'us-1east.pem'
region_default = 'us-east-1'
metadataFile_default  = 'MetaData.csv'
stackName_default = "Stack"+str(datetime.datetime.now().strftime("%f"))

#######Fabric####################
user = 'ec2-user'
keyFile = 'us-1east.pem'


#Get Arguments
def getArgs():
	# Assign description to the help doc
	parser = argparse.ArgumentParser(
	    description='Arguments for the AWS automation script')
	# Add arguments
	parser.add_argument(
	    '-awsAccessId', '--awsAccessId', type=str, help='AWS AccessId of your account', default=awsAccessId_default)
	parser.add_argument(
	    '-awsAccessKey', '--awsAccessKey', type=str, help='AWS AccessKey of your account', default=awsAccessKey_default)
	parser.add_argument(
	    '-keyFile', '--keyFile', type=str, help='Key File for the instance', default=keyFile_default)
	parser.add_argument(
	    '-region', '--region', type=str, help='Region of the instance', default=region_default)
	parser.add_argument(
	    '-metadataFile', '--metadataFile', type=str, help='Metadata File', default=metadataFile_default)
	parser.add_argument(
	    '-stackName', '--stackName', type=str, help='Name of CloudFormation Stack', default=stackName_default)
	# Array for all arguments passed to script
	args = parser.parse_args()
	# Assign args to variables
	awsAccessId = args.awsAccessId
	awsAccessKey = args.awsAccessKey
	keyFile = args.keyFile
	region = args.region
	metadataFile = args.metadataFile
	stackName = args.stackName
	# Return all variable values
	return awsAccessId, awsAccessKey, keyFile, region, metadataFile, stackName

#Create CloudFormation Stack
def createStackCloudformation(templatebody):
	print ("Creating the CloudFormation stack")
	cloudFormationClient = boto3.client('cloudformation',
			aws_access_key_id = awsAccessId,
			aws_secret_access_key = awsAccessKey,
			region_name = region)
	response = cloudFormationClient.create_stack(
	StackName = stackName,
	TemplateBody=templatebody
	)
	waiter = cloudFormationClient.get_waiter('stack_create_complete')
	waiter.wait(
		StackName = stackName
	)

#Create Json Template
def createJsonCloudformation():
	#Read the MetaData from csv
	instances = []
	imageIDs = []
	securityGroupIds = []
	subnetIds = []
	keyFiles = []
	instanceNames = []

	with open(metadataFile) as csvfile:
		readCSV = csv.DictReader(csvfile)

		for row in readCSV:
			#Check if there are any NULL entires in the Meatadata.
			if row['ImageID'] == "" or row['InstanceType'] == "" or \
				row['SecurityGroup'] == "" or row['SubnetID'] == "" or \
				row['KeyFile'] == "" or row['InstanceName'] == "":

				print ("Please check the Metadata CSV and make sure there are no NULL entries")
				exit()

			imageIDs.append(row['ImageID'])
			instances.append(row['InstanceType'])
			securityGroupIds.append(row['SecurityGroup'])
			subnetIds.append(row['SubnetID'])
			keyFiles.append(row['KeyFile'])
			instanceNames.append(row['InstanceName'])

	#Create the json
	finalJson=' {"AWSTemplateFormatVersion" : "2010-09-09","Description" : "A simple EC2 instance","Resources" : {'
	for index in range(0,len(instances)):
		tempJson = '"MyEC2Instance'+str(index+1)+ \
			'":{"Type":"AWS::EC2::Instance","Properties" :{ "ImageId" : "'+ \
			imageIDs[index]+'","InstanceType" :"'\
			+ instances[index] + '", "KeyName" : "'+ keyFiles[index]+ \
			'","SecurityGroupIds" : [ "' + securityGroupIds[index] + \
			'" ],"SubnetId" : "' + subnetIds[index] + \
			'", "Tags" :[ {"Key" : "Name", "Value" :"' + \
			instanceNames[index]+ '" } ]} }'

		finalJson = finalJson+tempJson+','

	finalJson = finalJson[:-1]+'} }'
	print (finalJson)
	return finalJson

#Get the instance Ids of the created instances
def getInstanceIDs():
	ec2Client = boto3.client('ec2',aws_access_key_id = awsAccessId, aws_secret_access_key = awsAccessKey ,region_name = region)
	response = ec2Client.describe_instances()
	instanceIDs = []

	#Get the instances from the MetaData files
	instanceNames = []
	with open(metadataFile) as csvfile:
		readCSV = csv.DictReader(csvfile)
		for row in readCSV:
			instanceNames.append(row['InstanceName'])

	#Get the IP address of the instances listed in the MetaData File
	for instance in response['Reservations']:
		for individualInstance in instance['Instances']:
			try:
				#Condition to check if the instance is running
				if 'running' in individualInstance['State']['Name']:
					for tags in individualInstance['Tags']:
						for instanceName in instanceNames:
							if instanceName in tags['Value']:
								#Append the Instance IDs to a list
								instanceIDs.append(individualInstance['InstanceId'])
			except Exception:
				pass
	return instanceIDs

#Check the intsance statuses of the newly created instances
def checkInstanceStatus(instanceIDs):
	print ("Waiting for the instances to get created")
	for instanceID in instanceIDs:
		ec2Client = boto3.client('ec2',aws_access_key_id = awsAccessId, aws_secret_access_key = awsAccessKey ,region_name = region)
		waiter = ec2Client.get_waiter('instance_status_ok')
		waiter.wait(
			InstanceIds=[instanceID]
		)

#Get the list of the public IP addresses of the newly craeted instances
def getPublicIPs():
	outputFile = open('publicIPs.txt', 'w')
	ec2Client = boto3.client('ec2',aws_access_key_id = awsAccessId, aws_secret_access_key = awsAccessKey ,region_name = region)
	response = ec2Client.describe_instances()
	ec2Instances = {}
	publicIPs = []
	instanceIDs = []

	#Get the instances from the MetaData files
	instanceNames = []
	with open(metadataFile) as csvfile:
		readCSV = csv.DictReader(csvfile)
		for row in readCSV:
			instanceNames.append(row['InstanceName'])

	#Get the IP address of the instances listed in the MetaData File
	for instance in response['Reservations']:
		for individualInstance in instance['Instances']:
			try:
				#Condition to check if the instance is running
				if 'running' in individualInstance['State']['Name']:
					for tags in individualInstance['Tags']:

						for instanceName in instanceNames:
							if instanceName in tags['Value']:
								#Append the Instance IDs to a list
								instanceIDs.append(individualInstance['InstanceId'])
								#Dictionary containing InstanceName and PublicIpAddress
								ec2Instances[tags['Value']] = individualInstance['PublicIpAddress']
								#Append the public IP address to a list
								publicIPs.append(individualInstance['PublicIpAddress'])
								outputFile.write("%s\n" % individualInstance['PublicIpAddress'])
			except Exception:
				pass
	print(publicIPs)
	return publicIPs

#INvoke the Bash script to configure Passwordless SSH
def invokePasswordlessSSHBash():
	print("Invoking Bash Script for Passwordless SSH")
	P = subprocess.Popen(["bash", "create_ssh_keys.bash", "publicIPs.txt", keyFile])
	P.communicate("exit")


################################################################################
#        Fabric
################################################################################

#Install a package on any instance
def installPackage(user,packageName,publicIPs,keyFile):
    for IPAddress in publicIPs:
        connection = Connection(host=IPAddress, user=user, connect_kwargs = {'key_filename': ['' + keyFile + ''] })
        connection.run('sudo yum -y install %s' % packageName, pty=True)


#Copy a file to any instance
def copyFile(user,fileName,destinationPath,publicIPs,keyFile):
    for IPAddress in publicIPs:
        print IPAddress
        connection = Connection(host=IPAddress, user=user, connect_kwargs = {'key_filename': ['' + keyFile + ''] })
        connection.run('ls -ltr',pty=True)
        connection.put(fileName,remote=destinationPath+fileName,preserve_mode=True)


#Copy a directory to any instance
def copyDirectory(user,directoryName,destinationPath,publicIPs,keyFile):
    for IPAddress in publicIPs:
		config = Config({'identity':'us-1east.pem'})
		connection = Connection(host=IPAddress, user=user, config=config, connect_kwargs = {'key_filename': ['' + keyFile + ''] } )

		filePaths = []
		connection.run('mkdir '+directoryName+'')
		for root, directories, filenames in os.walk(directoryName):
			#Directories
			for directory in directories:
				directoryPath = os.path.join(root, directory)
				index = directoryPath.find('/')
				print directoryPath[index+1:]
				connection.run('mkdir '+directoryPath+'')

			for filename in filenames:
				filePaths.append(os.path.join(root,filename))

		for filePath in filePaths:
			connection.put(filePath,remote=filePath,preserve_mode=True)

def passwordlessSSH(user,publicIPs,keyFile,cwd):
	#Change the permissions of authorized_keys to 600
	subprocess.check_output(['bash','-c', "chmod 600 keys/authorized_keys_updated"])

	print("\nCopy the Authorized Keys file to all instances")
	for IPAddress in publicIPs:
		print IPAddress
		connection = Connection(host=IPAddress, user=user, connect_kwargs = {'key_filename': ['' + keyFile + ''] })
		connection.put(''+cwd +'/keys/authorized_keys_updated',remote='/home/'+user+'/.ssh/authorized_keys',preserve_mode=True)

#Create the public key using ssh-keygen
def createKeys(user,publicIPs,destinationPath,keyFile):
	print("\nCreate the keys for each instance")
	#Remove files if they exists
	if os.path.exists('keys/id_rsa.pub'): os.remove('keys/id_rsa.pub')
	if os.path.exists('keys/authorized_keys'): os.remove('keys/authorized_keys')
	if os.path.exists('keys/authorized_keys_updated'): os.remove('keys/authorized_keys_updated')

	for IPAddress in publicIPs:
		print IPAddress
		connection = Connection(host=IPAddress, user=user, connect_kwargs = {'key_filename': ['' + keyFile + ''] })

		if checkFileExists(user, IPAddress, keyFile, '/home/'+ user +'/.ssh/id_rsa.pub'):
			print("Key already exists for {}".format(IPAddress))
		else:
			connection.run("echo -ne '\n' | ssh-keygen -t rsa")

		connection.get('/home/'+user+'/.ssh/authorized_keys',local=destinationPath+'authorized_keys')
		connection.get('/home/'+user+'/.ssh/id_rsa.pub',local=destinationPath+'id_rsa.pub')

		#Write the keys to authorized keys updated file
		keyFilePath = open('keys/id_rsa.pub')
		authorizedKeyFile = open('keys/authorized_keys')
		authorizedKeyFileUpdated = open('keys/authorized_keys_updated','a')

		for line in keyFilePath:
			authorizedKeyFileUpdated.write(line)
		for line in authorizedKeyFile:
			authorizedKeyFileUpdated.write(line)

def checkFileExists(user, IPAddress, keyFile, filePath):
	connection = Connection(host=IPAddress, user=user, connect_kwargs = {'key_filename': ['' + keyFile + ''] })
	try:
		result = connection.run('ls '+ filePath +'')
	except Exception:
		return False

	return True

#Disable SELinux
def disableSELinux(user,publicIPs,keyFile):
	for IPAddress in publicIPs:
		connection = Connection(host=IPAddress, user=user, connect_kwargs = {'key_filename': ['' + keyFile + ''] })
		connection.run("yes | sudo sed -i 's/SELINUX=enforcing/SELINUX=disabled/' /etc/selinux/config")
		print("SE Linux successfully disabled in {}".format(IPAddress))

#Flush IP Tables
def flushIPTables(user,publicIPs,keyFile):
	for IPAddress in publicIPs:
		connection = Connection(host=IPAddress, user=user, connect_kwargs = {'key_filename': ['' + keyFile + ''] })
		connection.run("sudo iptables -F")
		print("IP Tables Flushed on server on {}".format(IPAddress))

#Disable Firewall
def disableFirewall(user,publicIPs,keyFile):
	for IPAddress in publicIPs:
		connection = Connection(host=IPAddress, user=user, connect_kwargs = {'key_filename': ['' + keyFile + ''] })
		connection.run("yes | sudo yum install firewalld")
		connection.run("sudo systemctl disable firewalld")
		connection.run("sudo systemctl stop firewalld")
		print("Successfully disabled firewall on server {}".format(IPAddress))

#Configure vdbench accross all instances
def vdbenchConfigration(user,publicIPs,keyFile):
	installPackage(user,'firewalld',publicIPs,keyFile)
	installPackage(user,'java-1.8.0-openjdk*',publicIPs,keyFile)

    #Copy VdBench accross all instances
	copyDirectory(user,'vdbench50407','/home/ec2-user/',publicIPs,keyFile)

	#Get the current working directory
	cwd = os.getcwd()

	#Create Directory keys
	if not os.path.isdir(cwd+'/keys'):
		output = subprocess.check_output(['bash','-c', "mkdir keys"])
	else:
		print ("Keys Directory Exists")

    #Copy .pem files and configure Passwordless SSH
	createKeys(user,publicIPs,''+ cwd +'/keys/',keyFile)

    #Copy the keys accross all instances
	passwordlessSSH(user,publicIPs,keyFile,cwd)

	disableSELinux(user,publicIPs,keyFile)
	flushIPTables(user,publicIPs,keyFile)
	disableFirewall(user,publicIPs,keyFile)


if __name__== "__main__":

	#Get arguments
	awsAccessId, awsAccessKey, keyFile, region, metadataFile, stackName = getArgs()

	#Create Json Template
	jsonObject = createJsonCloudformation()

	#Create Stack
	createStackCloudformation(jsonObject)

	#Get the instance IDs
	instanceIds = getInstanceIDs()

	#Check if the instances are up and running. Will wait till they are running.
	checkInstanceStatus(instanceIds)

	#Get the Public IP's
	publicIPs = getPublicIPs()

	#Invoke the passwordless ssh script
	#invokePasswordlessSSHBash()

	#Configure vdbench accross all instances and configure them
	vdbenchConfigration(user,publicIPs,keyFile)
