#!/usr/bin/python
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
import yaml
import json
import StringIO

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
        description='Arguments for the Vdbench automation script')

    group = parser.add_mutually_exclusive_group(required=True)
    #group.add_argument('-f','--JSONConfig',action='store_const',const=True)
    group.add_argument('-d','--JSONConfigTemplateDump',type=str,help='Dump the JSON template in the file provided')
    group.add_argument('-m','--JSONConfigMenu',action='store_const',const=True, help='Provide the configrations via menu')
    group.add_argument('-f','--JSONConfig',type=str,help='Provide the JSON configration file')

    # Array for all arguments passed to script
    args = parser.parse_args()
    if args.JSONConfig:
        parseJSON(args.JSONConfig)
    elif args.JSONConfigTemplateDump:
        blankJSONTemplate(args.JSONConfigTemplateDump)
    elif args.JSONConfigMenu:
        manualConfigrationMenu()

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

def addUserPrivileges(user,publicIPs,KeyFile):
	for IPAddress in publicIPs:
		connection = Connection(host=IPAddress, user=user, connect_kwargs = {'key_filename': ['' + keyFile + ''] })
		connection.run("sudo usermod -g 0 -G 0 {}".format(user))
		print("Successfully added user {} to root priviledges on server {}".format(user,IPAddress))


#Configure vdbench accross all instances
def vdbenchConfigration(user,publicIPs,keyFile):
	installPackage(user,'firewalld',publicIPs,keyFile)
	installPackage(user,'java-1.8.0-openjdk*',publicIPs,keyFile)

    #Copy VdBench accross all instances
	#copyDirectory(user,'vdbench50407','/home/ec2-user/',publicIPs,keyFile)

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
	addUserPrivileges(user,publicIPs,keyFile)

################################################################################
# Congigure Vdbench parameters
################################################################################

def createHostFile(user,privateIPs):
    print("\nCreating the hosts config file")
    if os.path.exists('hosts-nfs'): os.remove('hosts-nfs')
    hostFile = open('hosts-nfs','a')
    hostFile.write('hd=default,jvms=8,shell=ssh\n')
    count=1
    for IPAddress in privateIPs:
        hostFile.write("hd=slave{},system={},user={}\n".format(count,IPAddress,user))
        count=count+1

    hostFile.close()
    print("Created the host configuration file {}".format('hosts-nfs'))


def createLUNFile(user,privateIPs,LUNs):
    print("\nCreating the LUN config file")
    if os.path.exists('luns-nfs'): os.remove('luns-nfs')

    lunFile = open('luns-nfs','a')
    lunFile.write('sd=default\n')
    count=1
    for lun in LUNs:
    	lunFile.write("sd=sd{},host=slave{},lun={}/file-*,openflags=o_direct\n".format(count,count,lun))
    	count=count+1

    lunFile.close()
    print("Created the LUN configuration file {}".format('luns-nfs'))

def createVdBenchConfig(user,hosts,volumes,keys,workloads,workloadDefinitionsManual,runDefaultsManual,runDefinitionsManual):
    print("\n\nCreating the Vdbench config file")

    if os.path.exists('vdbench-config'): os.remove('vdbench-config')

    vdbenchConfigFile = open('vdbench-config','a')

    #Host Definitions
    vdbenchConfigFile.write("*	HOST DEFINITIONS\n")
    vdbenchConfigFile.write("include=hosts-nfs\n\n")

    #Storage Definitions
    vdbenchConfigFile.write("*	STORAGE DEVICE DEFINITIONS\n")
    vdbenchConfigFile.write("include=luns-nfs\n\n")

    workloadDefinitions = StringIO.StringIO()
    runDefaults = StringIO.StringIO()
    runDefinitions = StringIO.StringIO()

    #Workload Definition
    for workload in workloads:
        if workload == '4corners':
            #Workload Definition
            workloadDefinitions.write("\n\n*	WORKLOAD DEFINITIONS FOR BASIC\n\n")
            workloadDefinitions.write("\n* random 4k workloads\n")
            workloadDefinitions.write("wd=RndRead,sd=sd*,rdpct=100,seekpct=100,xfersize=4k\n")
            workloadDefinitions.write("wd=RndWrite,sd=sd*,rdpct=0,seekpct=100,xfersize=4k\n")
            workloadDefinitions.write("\n* Seq workloads\n")
            workloadDefinitions.write("wd=SeqRead,sd=sd*,rdpct=100,seekpct=.1,xfersize=32k\n")
            workloadDefinitions.write("wd=SeqWrite,sd=sd*,rdpct=0,seekpct=.1,xfersize=32k\n")

            #Run Defaults
            runDefaults.write("\n\n*	RUN DEFAULTS FOR BASIC\n")
            runDefaults.write("rd=default,curve=(10-120,10),iorate=curve,warmup=30,elapsed=30\n")

            #Run Definitions
            runDefinitions.write("\n\n*	RUN DEFINITIONS FOR BASIC\n")
            runDefinitions.write("*** Random Read Test -  Curve")
            runDefinitions.write('rd=RndRead-4k,wd=RndRead,sd=("sd*"),threads=36\n')

            runDefinitions.write("\n*** Random Write Test  -  Curve\n")
            runDefinitions.write('rd=RndWrite-4k,wd=RndWrite,sd=("sd*"),threads=14\n')

            runDefinitions.write("\n*** SeqRead 32k -  Curve\n")
            runDefinitions.write('rd=SeqRead-32k,wd=SeqRead,sd=("sd*"),threads=10\n')

            runDefinitions.write("\n*** SeqRead 64k -  Curve\n")
            runDefinitions.write('rd=SeqRead-64k,wd=SeqRead,sd=("sd*"),threads=8\n')

            runDefinitions.write("\n*** SeqWrite 32k -  Curve\n")
            runDefinitions.write('rd=SeqWrite-32k,wd=SeqWrite,sd=("sd*"),threads=8\n')

            runDefinitions.write("\n*** SeqWrite 64k -  Curve\n")
            runDefinitions.write('rd=SeqWrite-64k,wd=SeqWrite,sd=("sd*"),threads=8\n')

        elif workload == 'oracle':
            #Workload Definition
            workloadDefinitions.write("\n\n*	WORKLOAD DEFINITIONS FOR ORACLE\n\n")
            workloadDefinitions.write("wd=Oracle-80-20,sd=sd*,rdpct=80,seekpct=100,xfersize=8192\n")
            workloadDefinitions.write("wd=Oracle-90-10,sd=sd*,rdpct=90,seekpct=100,xfersize=8192\n")
            workloadDefinitions.write("wd=wd-Oracle-8192,sd=sd*,rdpct=80,seekpct=100,xfersize=8192,skew=99\n")
            workloadDefinitions.write("wd=wd-Oracle-65536,sd=sd*,rdpct=1,seekpct=0.1,xfersize=65536,skew=1\n")

            #Run Defaults
            runDefaults.write("\n\n*	RUN DEFAULTS FOR ORACLE\n")
            runDefaults.write("rd=default,curve=(10-120,10),warmup=30,elapsed=30,interval=5,iorate=curve\n")

            #Run Definitions
            runDefinitions.write("\n\n*	RUN DEFINITIONS FOR ORACLE\n")
            runDefinitions.write("\n*** Oracle 80/20 -  Curve\n")
            runDefinitions.write('rd=Oracle-80-20,wd=Oracle-80-20,sd=("sd*"),forthreads=20\n')

            runDefinitions.write("\n*** Oracle 90/10 -  Curve\n")
            runDefinitions.write('rd=Oracle-90-10,wd=Oracle-90-10,sd=("sd*"),forthreads=22\n')

            runDefinitions.write("\n*** Oracle with Skew -  Curve\n")
            runDefinitions.write('rd=OracleWorkload,wd=wd-Oracle*,sd=("sd*"),forthreads=24\n')

        elif workload == 'sqlserver':
            #Workload Definition
            workloadDefinitions.write("\n\n*	WORKLOAD DEFINITIONS FOR SQL SERVER\n\n")
            workloadDefinitions.write("wd=SQLworkload,sd=sd*,rdpct=97,seekpct=100,xfersize=8192\n")
            workloadDefinitions.write("wd=SQL-80-20,sd=sd*,rdpct=80,seekpct=100,xfersize=8192\n")
            workloadDefinitions.write("wd=SQL-90-10,sd=sd*,rdpct=90,seekpct=100,xfersize=8192\n")

            #Run Defaults
            runDefaults.write("\n\n*	RUN DEFAULTS FOR SQL SERVER\n")
            runDefaults.write("rd=default,curve=(10-120,10),warmup=30,elapsed=30,interval=5,iorate=curve\n")

            #Run Definitions
            runDefinitions.write("\n\n*	RUN DEFINITIONS FOR SQL SERVER\n")
            runDefinitions.write("\n*** SQL Workload\n")
            runDefinitions.write('rd=SQL-Workload,wd=SQLworkload,sd=("sd*"),threads=26\n')

            runDefinitions.write("\n*** SQL Workload of 80/20\n")
            runDefinitions.write('rd=SQL-80-20,wd=SQL-80-20,sd=("sd*"),threads=18\n')

            runDefinitions.write("\n*** SQL Workload of 90/20\n")
            runDefinitions.write('rd=SQL-90-10,wd=SQL-90-10,sd=("sd*"),threads=22\n')

        else:
            workloadDefinitions.write(workloadDefinitionsManual.getvalue())
            runDefaults.write(runDefaultsManual.getvalue())
            runDefinitions.write(runDefinitionsManual.getvalue())
    #Add all the string buffers to files
    vdbenchConfigFile.write(workloadDefinitions.getvalue())
    vdbenchConfigFile.write(runDefaults.getvalue())
    vdbenchConfigFile.write(runDefinitions.getvalue())

    print("Created the vdbench configuration file {}".format('vdbench-config'))

def readVdbenchConfig(metadataFile):
	print("\nCreating the Vdbench config file")
	#Read the MetaData from csv
	threads = []
	blockSize = []
	readPercentage = []
	seekPercentage = []

	with open(metadataFile) as csvfile:
		readCSV = csv.DictReader(csvfile)

		for row in readCSV:
			#Check if there are any NULL entires in the Meatadata.
			if row['Threads'] == "" or row['BlockSize'] == "" or \
				row['ReadPercentage'] == "" or row['SeekPercentage'] == "" :

				print ("Please check the Vdbench Metadata CSV and make sure there are no NULL entries")
				exit()

			threads.append(row['Threads'])
			blockSize.append(row['BlockSize'])
			readPercentage.append(row['ReadPercentage'])
			seekPercentage.append(row['SeekPercentage'])

	if os.path.exists('vdbench-config'): os.remove('vdbench-config')
	vdbenchConfigFile = open('vdbench-config','a')
	#Workload Definition
	for index in range(0,len(threads)):
		vdbenchConfigFile.write("wd=wd{},sd=sd*,rdpct={},seekpct={},xfersize={}\n".format(index,readPercentage[index],seekPercentage[index],blockSize[index]))

	#Run Defaults
	vdbenchConfigFile.write("rd=default,curve=(10-120,10),warmup=30,elapsed=30\n")

	#Run Definitions
	vdbenchConfigFile.write("rd=run1,wd=wd*,sd=sd*,iorate=curve,interval=5,forthreads={}".format(threads[0]))

def parseJSON(filePath):
    print filePath

    with open(filePath) as jsonFile:
        jsonObj = json.load(jsonFile)

        hosts = jsonObj['hosts']
        volumes = jsonObj['volumes']
        keys = jsonObj['keys']
        workloads = jsonObj['workloads']
        createHostFile(user,hosts)
        createLUNFile(user,hosts,volumes)
        createVdBenchConfig(user,hosts,volumes,keys,workloads)

def blankJSONTemplate(filePath):
    jsonConfig = open(filePath,'w')

    jsonDict = {}
    jsonDict['hosts'] = []
    jsonDict['volumes'] = []
    jsonDict['keys'] = []
    jsonDict['workloads'] = []
    print(jsonDict)
    jsonContent = json.dumps(jsonDict,indent=4)
    jsonConfig.write(jsonContent)
    print("Created the JSON configration template at {}".format(filePath))

def parseYaml(filePath):
    print filePath
    stream = file(filePath,'r')
    yamlObj = yaml.load(stream)
    return yamlObj['hosts'], yamlObj['volumes'], yamlObj['keys'], yamlObj['workloads']

def mainMenu():
    print("\nWelcome to Vdbench Automator")
    print("Please enter your choice\n")
    print("1. Create configration")
    print("2. Provide the JSON config file") #argparse
    print("3. Create a blank JSON config template") #argparse
    print("Press any other key to exit the menu\n")

    while True:
        choice = input("Please enter your choice: ")
        if choice == 1:
            return manualConfigrationMenu()
            break
        elif choice == 2:
            #return YAMLConfigFile()
            return JSONConfigFile()
            break
        elif choice == 3:
            blankYAMLTemplate()
            break
        else:
            break

def manualConfigrationMenu():
    hosts = []
    volumes = []
    keys = []
    workloads = []

    workloadDefinitions = StringIO.StringIO()
    runDefaults = StringIO.StringIO()
    runDefinitions = StringIO.StringIO()

    print("\n************* You have entered choice manual configuration *************")
    print("\nPlease enter your choice\n")
    print("1. Add Hosts")
    print("2. Add Volumes")
    print("3. Add Keys")
    print("4. Add Workloads")
    print("5. See menu")
    print("Press any other key to exit the menu\n")

    while True:
        choice = input("\nPlease enter your choice (Enter 5 to see the menu): ")
        if choice==1:
            print("Enter the Host Private IP address (Enter Ctrl-D to exit): ")
            while(True):
                try:
                    hostip = raw_input()
                    hosts.append(hostip)
                except EOFError:
                    break
            print("Added hosts {}".format(hosts))

        elif choice==2:
            print("Enter the volumes (Enter Ctrl-D to exit): ")
            while(True):
                try:
                    volume = raw_input()
                    volumes.append(volume)
                except EOFError:
                    break
            print("Added volumes {}".format(volumes))

        elif choice==3:
            print("Enter the keys (Enter Ctrl-D to exit): ")
            while(True):
                try:
                    key = raw_input()

                    #Check if the file Exists
                    if not os.path.exists(key):
                        print("File {} doesnot exist".format(key))
                    else:
                        keys.append(key)
                except EOFError:
                    break
            print("Added keys {}".format(keys))

        elif choice==4:
            print("Enter the workload details[4corners/ oracle/ sqlserver/ manual]: ")
            print("Enter the workloads (Enter Ctrl-D to exit): ")
            while(True):
                try:
                    workload = raw_input()
                    workloads.append(workload)

                    if workload == 'manual':
                        print("\nPlease enter manual workload details\n")
                        #print("Enter the workloads: ")
                        count = 1
                        workloadDefinitions.write("\n\n*	WORKLOAD DEFINITIONS FOR MANUAL\n")
                        runDefinitions.write("\n\n*	RUN DEFINITIONS FOR MANUAL\n")
                        runDefaults.write("\n\n*	RUN DEFAULTS FOR MANUAL\n")

                        while(True):
                            try:
                                print("\nEnter the configuration details")
                                readPercentage = raw_input("Enter read %: ")
                                seekPercentage = raw_input("Enter seek %: ")
                                blockSize = raw_input("Enter block size: ")

                                #Workload Definition
                                workloadDefinitions.write("wd=Manualworkload{},sd=sd*,rdpct={},seekpct={},xfersize={}\n".format(count,readPercentage,seekPercentage,blockSize))

                                #Run Defaults
                                runDefaults.write("rd=default,curve=(10-120,10),warmup=30,elapsed=30,interval=5,iorate=curve\n")

                                #Run Definitions
                                runDefinitions.write('rd=Manual-Workload{},wd=Manualworkload{},sd=("sd*"),threads=10\n'.format(count,count))
                                count+=1

                                choice = raw_input("Do you wish to add another configuration[y/n]: ")
                                if choice == 'n':
                                    break

                            except EOFError:
                                break
                except EOFError:
                    break
            print("Added workloads {}".format(workloads))
        elif choice==5:
            print("1. Add Hosts")
            print("2. Add Volumes")
            print("3. Add Keys")
            print("4. Add Workloads")
            print("Press any other key to exit the menu\n")
        else:
            break

    print hosts
    print volumes
    print keys
    print workloads

    #Create the JSON config file
    jsonConfig = open('vdbench_config.json','w')

    jsonDict = {}
    jsonDict['hosts'] = hosts
    jsonDict['volumes'] = volumes
    jsonDict['keys'] = keys
    jsonDict['workloads'] = workloads
    print(jsonDict)
    jsonContent = json.dumps(jsonDict,indent=4)
    jsonConfig.write(jsonContent)

    createHostFile(user,hosts)
    createLUNFile(user,hosts,volumes)
    createVdBenchConfig(user,hosts,volumes,keys,workloads,workloadDefinitions,runDefaults,runDefinitions)
    return hosts,volumes,keys,workloads



def YAMLConfigFile():
    print("\nYou have entered choice YAML configuration file")
    YAMLFilePath = raw_input("Enter the YAML config file path: ")
    return parseYaml(YAMLFilePath)

def JSONConfigFile():
    print("\n************* You have entered choice JSON configuration file *************")
    JSONFilePath = raw_input("\nEnter the JSON config file path: ")
    return parseJSON(JSONFilePath)

if __name__== "__main__":

    #Get arguments
    getArgs()
