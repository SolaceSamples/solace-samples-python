# Solace Samples Python

## Environment Setup
1. Install Python 3.7 (See installed version using `python3 -V`)
1. [Optional] Install virtualenv `python3 -m pip install --user virtualenv`
1. Clone this repository
1. [Optional] Setup python virtual environment `python3 -m venv venv`
1. [Optional] Activate virtual environment `source venv/bin/activate`

## Install Dependencies 
1. `pip install -r requirements.txt` (See [Notes](#Notes) if this fails)

## Run Samples
Execute the script of choice and pass the environment variables

`HOST=<host_name> VPN=<vpn_name> SOL_USERNAME=<username> SOL_PASSWORD=<password> python <name_of_file>.py`

## Notes:
1. Python Virtual environments are recommended to keep your project dependencies within the project scope and avoid polluting global python packages
1. Solace hostname, username, message vpn, and password are obtained from your Solace cloud account
1. If you are reading this and the Solace Python API is still not published, you can install the wheel package from the [Solace Community](https://solace.community/discussion/336/python-whos-in-for-a-real-treat)

## To-Do
- [x] Add loop for publisher message rate
- Add License, Authors, Contributing

## Resources
- Solace Developer Portal is at [solace.dev](https://solace.dev)
- Ask the [Solace Community](https://solace.community)
