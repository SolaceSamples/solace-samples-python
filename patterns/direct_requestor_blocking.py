## Goal: A simpe request pulblisher to publish a request with reply_to set to any topic.
# Unlike regular publisher, this will be built on RequestReplyMessagePublisher which exposes the ability to set 'Reply To' field
import os
import platform
import time
import calendar;

# Import Solace Python  API modules from the solace package
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, \
            ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.resources.topic import Topic
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError

if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1" # Disable stdout buffer 

# Goal is to demonstrate request-reply pattern using Direct messages. A request message carrying {name} is published,
# and a response with a "Greetings {name}" is expected as response.
#   direct_requestor: publishes('solace/samples/python/direct/hello/{name}, and waits for response (either synchronously or asynchronously)
#   direct_replier: subscribes('solace/samples/python/direct/hello/>'), and replies with a greetings message in the body.

TOPIC_PREFIX = "solace/samples/python"

name = ""
while not name:
    name = input("Enter your name: ")
unique_name = name.replace(" ", "")

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

# Event Handling for the messaging service
service_handler = ServiceEventHandler()
messaging_service.add_reconnection_listener(service_handler)
messaging_service.add_reconnection_attempt_listener(service_handler)
messaging_service.add_service_interruption_listener(service_handler)

# Create a direct message requestor and register the error handler
direct_requestor_blocking = messaging_service.request_reply() \
                                    .create_request_reply_message_publisher_builder() \
                                    .build()

# Blocking Start thread
direct_requestor_blocking.start()
print(f'\nDirect Requestor ready? {direct_requestor_blocking.is_ready()}')

# Prepare outbound message payload and body
message_body = "Hi, My name is " + f'{name}'
outbound_msg_builder = messaging_service.message_builder() \
                .with_property("application", "samples") \
                .with_property("language", "Python")

# Capture the timestamp and use that as message-id
gmt = time.gmtime()
message_id = calendar.timegm(gmt)

print('\nSend a KeyboardInterrupt to stop publishing')
try: 
    print(f'============================')
    topic = Topic.of(TOPIC_PREFIX + '/direct/hello/'  + f'{unique_name}')
    print(f'Publishing to topic:\n{topic}')

    try:
        # Direct publish the message with dynamic headers and payload
        outbound_msg = outbound_msg_builder \
                            .with_application_message_id(f'NEW {message_id}')\
                            .build(f'\n{message_body}')
        print(f'>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        print(f'Publishing request (body):' + outbound_msg.get_payload_as_string())
        print(f'----------------------------')
        print(f'Publishing message:\n{outbound_msg}')

        response = direct_requestor_blocking.publish_await_response(request_message=outbound_msg, \
                                                                    request_destination=topic, \
                                                                    reply_timeout=10000)
        print(f'<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        print(f'Received reply (body):\n' + response.get_payload_as_string())
        print(f'----------------------------')
        print(f'Received reply:\n{response}')
        print(f'============================\n')     
    except KeyboardInterrupt:
        print('\nInterrupted, disconnecting Messaging Service')
    except PubSubPlusClientError as exception:
        print(f'Received a PubSubPlusClientException: {exception}')
finally:
    print('\nTerminating Requestor')
    direct_requestor_blocking.terminate()
    print('\nDisconnecting Messaging Service')
    messaging_service.disconnect()