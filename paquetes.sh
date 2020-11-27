sudo apt -y update
sudo apt -y install python3-venv
cd /home/ubuntu/Api_new_york_times
python3 -m venv venv
source venv/bin/activate
pip install requests
pip install boto3