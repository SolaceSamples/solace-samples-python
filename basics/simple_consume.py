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
        payload_as_bytes = message.get_payload_as_bytes()
        payload_as_string = message.get_payload_as_string()
        correlation_id = message.get_correlation_id()
        print("\n" + f"CALLBACK: Message Received on Topic: {topic}.\n"
                     f"Message Bytes: {payload_as_bytes} \n"
                     f"Message String: {payload_as_string} \n"
                     f"Correlation id: {correlation_id}")

def direct_message_consume(messaging_service: MessagingService, topic_subscription: str):
    try:
        topics = [TopicSubscription.of(topic_subscription)]

        # Create a direct message consumer service with the topic subscription and start it
        direct_receive_service = messaging_service.create_direct_message_receiver_builder()
        direct_receive_service = direct_receive_service.with_subscriptions(topics).build()
        direct_receive_service.start()

        # Register a callback message handler
        direct_receive_service.receive_async(MessageHandlerImpl())
        print(f"Subscribed to: {topic_subscription}")
        # Infinite loop until Keyboard interrupt is received
        try: 
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print('\nDisconnecting Messaging Service')
    finally:
        messaging_service.disconnect()
        direct_receive_service.terminate()

# Broker Config
broker_props = {
    "solace.messaging.transport.host": os.environ['HOST'],
    "solace.messaging.service.vpn-name": os.environ['VPN'],
    "solace.messaging.authentication.scheme.basic.user-name": os.environ['SOL_USERNAME'],
    "solace.messaging.authentication.scheme.basic.password": os.environ['SOL_PASSWORD']
    }


# Initialize A messaging service + Connect to the broker
messaging_service = MessagingService.builder().from_properties(broker_props).build()
messaging_service.connect_async()

# Define a Topic subscription 
topic = "taxinyc/ops/ride/>"

# Direct publish the message
direct_message_consume(messaging_service, topic)