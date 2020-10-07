import os
import time
import pysolace

MSG_COUNT = 5

# Broker Config
broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOL_HOST') or "localhost",
    "solace.messaging.service.vpn-name": os.environ.get('SOL_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.user-name": os.environ.get('SOL_USERNAME') or "default",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOL_PASSWORD') or "default"
    }

# Initialize A messaging service + Connect to the broker
messaging_service = pysolace.messaging.messaging_service.MessagingService.builder().from_properties(broker_props).build()
messaging_service.connect_async()

# Create a direct message publish service and start it
direct_publish_service = messaging_service.create_direct_message_publisher_builder().build()
direct_publish_service.start_async()

# Prepare outbound message payload and body
message_body = "this is the body of the msg"
outbound_msg = messaging_service.message_builder() \
                .with_application_message_id("sample_id") \
                .with_property("application", "samples") \
                .with_property("language", "Python") \
                .build(message_body)
count = 1
print("\nSend a KeyboardInterrupt to stop publishing\n")
try: 
    while True:
        while count <= MSG_COUNT:
            # Build a dynamic topic
            topic = pysolace.messaging.utils.topic.Topic.of("taxinyc/ops/ride/called/v1" + f'/{count}')
            # Direct publish the message
            direct_publish_service.publish(destination=topic, message=outbound_msg)
            print(f'Published message on {topic}')
            count += 1
            time.sleep(0.1)
        count = 1
        print("\n")
        time.sleep(1)
except KeyboardInterrupt:
    print('\nTerminating Direct Publish Messaging Service')
    direct_publish_service.terminate()
    print('\nDisconnecting Messaging Service')
    messaging_service.disconnect()