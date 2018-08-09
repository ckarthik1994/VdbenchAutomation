# VdbenchAutomation
Automate the Vdbench setup workflow, reducing the setup time from 2 hours to 10 minutes. This automation will setup the end to end environment, from creating to EC2 instances to installing all the required dependencies for Vdbench. This workflow helps reduce the manual efforts to set up an Vdbench environment.

### Prerequisites: 
Please install boto3 and Fabric before running the scripts

sudo install pip
sudo pip install virtualenv
virtualenv vdbench_automation
 source vdbench/bin/activate

sudo pip install boto3
sudo pip install fabric

### Getting Set Up
1.	Fork this repository on GitHub.
2.	Clone your forked repository (not our original one) to your hard drive with git clone https://github.com/YOURUSERNAME/aws_deploy.git
3.	cd 


### Important Files:
•	MetaData.csv : This files stores the configurable parameters for the AWS EC2 instances. </br>
•	vdbench_environment_setup.py : This python file has all the commands for the automation.

### Running the automation

usage: vdbench_environment_setup.py [-h] [-awsAccessId AWSACCESSID] </br>
                               [-awsAccessKey AWSACCESSKEY] [-keyFile KEYFILE] </br>
                               [-region REGION] [-metadataFile METADATAFILE] </br>
                               [-stackName STACKNAME] </br></br>

Arguments for the AWS automation script </br>
optional arguments: </br>
  -h, --help            show this help message and exit </br>
  -awsAccessId AWSACCESSID, --awsAccessId AWSACCESSID </br>
                        AWS AccessId of your account </br>
  -awsAccessKey AWSACCESSKEY, --awsAccessKey AWSACCESSKEY 
                        AWS AccessKey of your account </br>
  -keyFile KEYFILE, --keyFile KEYFILE </br>
                        Key File for the instance </br>
  -region REGION, --region REGION </br>
                        Region of the instance </br>
  -metadataFile METADATAFILE, --metadataFile METADATAFILE </br>
                        Metadata File </br>
  -stackName STACKNAME, --stackName STACKNAME </br>
                        Name of CloudFormation Stack </br>

