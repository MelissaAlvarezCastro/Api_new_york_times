sudo apt -y update
cd /home/ubuntu/Api_new_york_times
mkdir consultas
chmod 700 consultas
sudo apt -y install python3-venv
python3 -m venv venv
source venv/bin/activate
pip install requests
pip install boto3