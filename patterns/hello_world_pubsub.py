## Goal: Publisher + Subscriber 
import os
import time

# Import Solace Python  API modules
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler
# from solace.messaging.core.solace_message import SolaceMessage
from solace.messaging.resources.topic import Topic

TOPIC_PREFIX = "samples/hello"
SHUTDOWN = False

# Handle received messages
class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: 'InboundMessage'):
        global SHUTDOWN
        if "quit" in message.get_destination_name():
            print("QUIT message received, shutting down.")
            SHUTDOWN = True 
        print("\n" + f"Message dump: {message.solace_message.get_message_dump()} \n")

# Inner classes for error handling
class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")
    
    def on_reconnecting(self, e: "ServiceEvent"):
        print("\non_reconnecting")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_service_interrupted(self, e: "ServiceEvent"):
        print("\non_service_interrupted")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"):
        print("on_failed_publish")

# Broker Config
broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOLACE_HOST') or "localhost",
    "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.username": os.environ.get('SOLACE_USERNAME') or "default",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOLACE_PASSWORD') or "default"
    }

# Build A messaging service with a reconnection strategy of 20 retries over an interval of 3 seconds
# Note: The reconnections strategy could also be configured using the broker properties object
messaging_service = MessagingService.builder().from_properties(broker_props)\
                    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                    .build()

# Blocking connect thread
messaging_service.connect()
# print(f'Messaging Service connected? {messaging_service.is_connected}')

# Error Handeling for the messaging service
service_handler = ServiceEventHandler()
messaging_service.add_reconnection_listener(service_handler)
messaging_service.add_reconnection_attempt_listener(service_handler)
messaging_service.add_service_interruption_listener(service_handler)

# Create a direct message publisher and start it
direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
direct_publisher.set_publish_failure_listener(PublisherErrorHandling())
direct_publisher.set_publisher_readiness_listener

# Blocking Start thread
direct_publisher.start()
# print(f'Direct Publisher ready? {direct_publisher.is_ready()}')

unique_name = ""
while not unique_name:
    unique_name = input("Enter your name: ").replace(" ", "")

# Define a Topic subscriptions 
topics = [TOPIC_PREFIX + "/python/>", TOPIC_PREFIX + "/control/>"]
topics_sub = []
for t in topics:
    topics_sub.append(TopicSubscription.of(t))

msgSeqNum = 0
# Prepare outbound message payload and body
message_body = f'Hello from Python Hellow World Sample!'
outbound_msg = messaging_service.message_builder() \
                .with_application_message_id("sample_id") \
                .with_property("application", "samples") \
                .with_property("language", "Python") \
                .build(message_body)
try:
    print(f"Subscribed to: {topics}")
    # Build a Receiver
    direct_receiver = messaging_service.create_direct_message_receiver_builder().with_subscriptions(topics_sub).build()
    direct_receiver.start()
    # Callback for received messages
    direct_receiver.receive_async(MessageHandlerImpl())
    if direct_receiver.is_running():
        print("Connected and Subscribed! Ready to publish\n")
    try:
        while not SHUTDOWN:
            # Direct publish the message
            direct_publisher.publish(destination=Topic.of(TOPIC_PREFIX + f"/python/{unique_name}/{msgSeqNum}"), message=outbound_msg)
            msgSeqNum += 1
            # Modifying the outbond message instead of creating a new one
            outbound_msg.solace_message.message_set_binary_attachment_string(f'{message_body} --> {msgSeqNum}')
            outbound_msg.solace_message.set_message_application_message_id(f'sample_id {msgSeqNum}')
            time.sleep(0.1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
    except PubSubPlusClientError as exception:
        print(f'Received a PubSubPlusClientException: {exception}')
finally:
    print('Terminating Publisher and Receiver')
    direct_publisher.terminate()
    direct_receiver.terminate()
    print('Disconnecting Messaging Service')
    messaging_service.disconnect()
