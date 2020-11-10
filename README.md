# Solace Samples Python

## Environment Setup
1. [Install Python 3.7](https://www.python.org/downloads/) (See installed version using `python3 -V`)   
    1.1 Note: If you are installing python for the first time on your machine then you can just use `python` instead of `python3` for the commands
1. [Optional] Install virtualenv `python3 -m pip install --user virtualenv`     
    1.1 Note: on a Linux machine, depending on the distribution you might need to `apt-get install python3-venv` instead
1. Clone this repository
1. [Optional] Setup python virtual environment `python3 -m venv venv`
1. [Optional] Activate virtual environment:     
    1.1 MacOS/Linux: `source venv/bin/activate`   
    1.2 Windows: `source venv/Scripts/activate`     

## Install the Solace Python API
1. Install the API `pip install solace-pubsubplus`

## Run Samples
Execute the script of choice as follows:

- `python <name_of_file>.py`

Note: This assumes you have a [local docker](https://solace.com/products/event-broker/software/getting-started/) broker running on localhost

To pass non default parameters, do so via the environment variables   
- `SOLACE_HOST=<host_name> SOLACE_VPN=<vpn_name> SOLACE_USERNAME=<username> SOLACE_PASSWORD=<password> python <name_of_file>.py`

## Notes:
1. [Python Virtual environment](https://docs.python.org/3/tutorial/venv.html) is recommended to keep your project dependencies within the project scope and avoid polluting global python packages
1. Solace hostname, username, message vpn, and password are obtained from your Solace cloud account

## To-Do
- [x] Add loop for publisher message rate
- [ ] Add License, Authors, Contributing
- [ ] Add `pip install pysolace` when API is released

## Resources
- Solace Developer Portal is at [solace.dev](https://solace.dev)
- Ask the [Solace Community](https://solace.community/discussion/336/python-whos-in-for-a-real-treat) for further discussions and questions.
