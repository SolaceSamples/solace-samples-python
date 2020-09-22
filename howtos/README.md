# Solace Sampler
The solace sampler demonstrate simple api usage.

## Running the samplers
All the sampler run using the following command:
``` python <path_to_file>/<sampler_file>.py ```
All samplers are named `how_to_*.py` with some sampler files under sub folders.
### Running all samplers
The sampler_master.py file can run all the sampler example programs.

### Connection configuration for samplers
All connection details are provided in the `solbroker_properties.json` file include the semp connection details and client connection details. With default value for a localhost deployment of the PubSubPlus software broker. Update the `solbroker_properties.json` file entries to point the sampler connections to a different broker.

### Secure Transport connections
The fixtures folder contains the sample client certificate api-client.pem and its CA public_root_ca.crt. Note the samplers autmatically adds a client certificate authority on the remote broker via SEMPv2.

TLS downgrade requires some manual remote broker configuration as well as the message vpn on the remote broker does not enable the feature to tls downgrade to plain text by default and must be confired. In the future this might be automatically enabled via SEMPv2 for the duration of the sampler execution.

Note the trusted certificate for the remote broker must be added the fixtures folder as that directory is the trustore to establish TLS connections for the `how_to*.py` samplers. For details on how to add a server certifcate to the broker see, https://docs.solace.com/Configuring-and-Managing/Managing-Server-Certs.htm.
For example should the broker have a server certificate derived from the public_root_ca.crt then TLS connection can be established since the public_root_ca.crt is in the truststore located at fixtures.
