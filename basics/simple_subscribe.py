import os
import time

# Import Solace Python  API modules from the pysolace package
from pysolace.messaging.messaging_service import MessagingService
from pysolace.messaging.utils.resources.topic_subscription import TopicSubscription
from pysolace.messaging.receiver.message_receiver import MessageHandler

# Callback functions 
class MessageHandlerImpl(MessageHandler):
    # Handle received messages
    def on_message(self, message: 'InboundMessage'):
        topic = message.get_destination_name()
        properties = message.get_properties()
        payload_str = message.get_payload_as_string()
        payload_bytes = message.get_payload_as_bytes()
        print("\n" + f"CALLBACK: Message Received on Topic: {topic}.\n"
                     f"Message String: {payload_str} \n"
                     f"Message Bytes: {payload_bytes} \n"
                     f"Message Properties: {properties} \n")

    # TO-Do: Handle errors
    
# Broker Config
broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOL_HOST') or "localhost",
    "solace.messaging.service.vpn-name": os.environ.get('SOL_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.user-name": os.environ.get('SOL_USERNAME') or "default",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOL_PASSWORD') or "default"
    }


# Initialize A messaging service + Connect to the broker
messaging_service = MessagingService.builder().from_properties(broker_props).build()
messaging_service.connect_async()

# Define a Topic subscriptions 
topics = ["taxinyc/ops/ride/>", "taxinyc/process/*"]

try:
    # Subscribe to the topic
    topics_sub = []
    for t in topics:
        topics_sub.append(TopicSubscription.of(t))
        print(f"Subscribing to: {t}")
    direct_receive_service = messaging_service.create_direct_message_receiver_builder()\
                            .with_subscriptions(topics_sub)\
                            .build()\

    direct_receive_service.start()
    direct_receive_service.receive_async(MessageHandlerImpl())
    try: 
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
finally:
    messaging_service.disconnect()
    direct_receive_service.terminate()
