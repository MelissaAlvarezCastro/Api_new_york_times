sudo apt -y update
cd /home/ubuntu
git clone https://github.com/MelissaAlvarezCastro/Api_new_york_times.git
cd /home/ubuntu/Api_new_york_times
sudo apt -y install python3-venv
python3 -m venv venv
source venv/bin/activate
pip install requests
pip install boto3