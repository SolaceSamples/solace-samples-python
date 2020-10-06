import os
import time

# Import Solace Python  API modules from the pysolace package
from pysolace.messaging.messaging_service import MessagingService
from pysolace.messaging.utils.resources.topic_subscription import TopicSubscription
from pysolace.messaging.receiver.message_receiver import MessageHandler

# Callback function to handle received messages
class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: 'InboundMessage'):
        topic = message.get_destination_name()
        # NOTE: Check type of msg before get as string
        payload_str = message.get_payload_as_string()
        # NOTE: Show how to receive header values
        print("\n" + f"CALLBACK: Message Received on Topic: {topic}.\n"
                     f"Message String: {payload_str} \n")

host = os.environ.get('SOL_HOST') or "localhost"
vpn_name = os.environ.get('SOL_VPN') or "default"
username = os.environ.get('SOL_USERNAME') or "default"
password = os.environ.get('SOL_PASSWORD') or "default"

# Broker Config
broker_props = {
    "solace.messaging.transport.host": host,
    "solace.messaging.service.vpn-name": vpn_name,
    "solace.messaging.authentication.scheme.basic.user-name": username,
    "solace.messaging.authentication.scheme.basic.password": password
    }


# Initialize A messaging service + Connect to the broker
messaging_service = MessagingService.builder().from_properties(broker_props).build()
messaging_service.connect_async()

# Define a Topic subscription 
topics = ["taxinyc/ops/ride/>", "taxinyc/analytics/ride/>"]

try:
    # Subscribe to the topic
    topics_sub = []
    for t in topics:
        topics_sub.append(TopicSubscription.of(t))
        print(f"Subscribing to: {t}")
    direct_receive_service = messaging_service.create_direct_message_receiver_builder().with_subscriptions(topics_sub).build().start()
    direct_receive_service.receive_async(MessageHandlerImpl())
    try: 
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
finally:
    messaging_service.disconnect()
    direct_receive_service.terminate()
