## Goal: A simpe request pulblisher to publish a request with reply_to set to any topic.
import os
import platform
import time

# Import Solace Python  API modules from the solace package
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, \
                                                ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener, FailedPublishEvent
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage


if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1" # Disable stdout buffer 

# Goal is to demonstrate receive-process-reply (processor) pattern. Processor subscribes to a request topic and upon receipt of 
# request message carrying a string, the processor converts the payload to uppercase (for simplicity) and publishes to a 
# designated topic.
#   direct_requestor: 
#       subscribes to request topic 'solace/samples/python/direct/processor/input/{count}
#       proceses the payload
#       publishes to reply topic 'solace/samples/python/direct/processor/output'

TOPIC_PREFIX = "solace/samples/python"

# Broker Config. Note: Could pass other properties Look into
broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOLACE_HOST') or "tcp://localhost:55555,tcp://localhost:55554",
    "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.username": os.environ.get('SOLACE_USERNAME') or "default",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOLACE_PASSWORD') or ""
    }

# Build A messaging service with a reconnection strategy of 20 retries over an interval of 3 seconds
# Note: The reconnections strategy could also be configured using the broker properties object
messaging_service = MessagingService.builder().from_properties(broker_props)\
                        .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                        .build()

# Blocking connect thread
messaging_service.connect()
print(f'\nMessaging Service connected? {messaging_service.is_connected}')

# Inner classes for connection error handling
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

# Event Handling for the messaging service
service_handler = ServiceEventHandler()
messaging_service.add_reconnection_listener(service_handler)
messaging_service.add_reconnection_attempt_listener(service_handler)
messaging_service.add_service_interruption_listener(service_handler)
 
# Inner classes for publish error handling
class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"):
        print("on_failed_publish")

# Handle received messages
class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: InboundMessage):
        out_topic = Topic.of(TOPIC_PREFIX + f'/direct/processor/output')
        in_topic = message.get_destination_name()
        payload_str = message.get_payload_as_string()
        print(f'Received input message (body string):\n{payload_str}')
        print(f'Message dump: {message.solace_message.get_message_dump()} \n')

        processed_payload_str = payload_str.upper()
        print(f'Published output message (body):\n{processed_payload_str}')

        output_msg = msg_builder \
                        .with_application_message_id(message.get_application_message_id())\
                        .build(f'{processed_payload_str}')

        direct_publisher.publish(destination=out_topic, message=output_msg)

        print(f'<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        print(f'Received input message on {in_topic}')
        print(f'Received input message (body):\n{payload_str}')
        print(f'----------------------------')
        print(f'>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print(f'Published output message on {out_topic}')
        print(f'Published output message (body):\n{processed_payload_str}')
        print(f'----------------------------')

# Create a direct message publisher and start it
direct_publisher = messaging_service.create_direct_message_publisher_builder().build()
direct_publisher.set_publish_failure_listener(PublisherErrorHandling())

# Blocking Start thread
direct_publisher.start()
print(f'Direct Publisher ready? {direct_publisher.is_ready()}')

# Define a Topic subscriptions 
topics = [TOPIC_PREFIX + "/direct/processor/input/>"]
topics_sub = []
for t in topics:
    topics_sub.append(TopicSubscription.of(t))
print(f'\nSubscribed to topic {topics}')

# Build a Receiver with the given topics and start it
direct_receiver = messaging_service.create_direct_message_receiver_builder()\
                        .with_subscriptions(topics_sub)\
                        .build()
direct_receiver.start()
print(f'Direct Subscriber is running? {direct_receiver.is_running()}')

# Prepare outbound message payload and body
msg_builder = messaging_service.message_builder() \
                .with_application_message_id("sample_id") \
                .with_property("application", "samples") \
                .with_property("language", "Python") \

count = 1
print("\nSend a KeyboardInterrupt to stop publishing\n")
try:
    # Callback for received messages
    direct_receiver.receive_async(MessageHandlerImpl())
    try: 
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nDisconnecting Messaging Service')
finally:
    print('\nTerminating receiver')
    direct_receiver.terminate()
    print('\nDisconnecting Messaging Service')
    messaging_service.disconnect()
