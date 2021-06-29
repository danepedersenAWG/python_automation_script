# Python Automation Script 
The aim of this script is to eliminate the need for manual testing.

This script will take the data from your .json file, send requests to the appropriate endpoints with the data, store the responses, and compare the data to show changes.  

First, make sure that you have the correct access to the appropriate endpoints. If you try to run this script and it times out, you most likely do not have the correct access. You will need to reach out to Derek to get added. 

# Setup
1. Install python3 with `yum install python3`. Verify that it installed correctly with `python3 --version`
2. Clone this repo with `git clone https://github.com/danepedersenAWG/python_automation_script.git`
3. run `python_automation_script`
4. run `pip3 install -r requirements.txt`

# Running the script
1. Copy the file you want to read into the root directory. It should be on the same level as the README and the script itself. Take a look at the way I have mine structured and try to get it as close to that as possible.
2. run `python3 --version`. I am using version 3.7
3. run `python3 testing_script.py`



