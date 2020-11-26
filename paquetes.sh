#!/bin/bash
sudo apt -y update
sudo apt-get -y install python3-pip
sudo apt -y install python3-venv
cd /home/ubuntu/Api_new_york_times
python3 -m venv venv
source venv/bin/activate
pip3 install requests
pip3 install boto3
sudo apt-get update
sudo apt-get upgrade