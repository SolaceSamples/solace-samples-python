## Goal: A simpe request pulblisher to publish a request with reply_to set to any topic.
# Unlike regular publisher, this will be built on RequestReplyMessagePublisher which exposes the ability to set 'Reply To' field
import os
import platform
import time

# Import Solace Python  API modules from the solace package
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, \
            ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener, FailedPublishEvent
from solace.messaging.publisher.request_reply_message_publisher import RequestReplyMessagePublisher

if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1" # Disable stdout buffer 

# The idea is to get the direct_requestor and direct_replier to work together asynchronously, 
# routing the replies to a specific topic that is consumed by direct_reply_receiver.
#   direct_requestor: publishes('solace/samples/python/direct/request/1')
#   direct_replier: subscribes('solace/samples/python/direct/request/1'), and replies('solace/samples/python/direct/reply/1')

MSG_COUNT = 10
TOPIC_PREFIX = "solace/samples/python"
REQUEST_TOPIC = "direct/request"
REPLY_TOPIC = "direct/reply"

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
direct_requestor = messaging_service.request_reply() \
                        .create_request_reply_message_publisher_builder() \
                        .build()

# Blocking Start thread
direct_requestor.start()
print(f'\nDirect Requestor ready? {direct_requestor.is_ready()}')

# Prepare outbound message payload and body
message_body = "this is the request body of the msg with count: "
outbound_msg_builder = messaging_service.message_builder() \
                .with_property("application", "samples") \
                .with_property("language", "Python")

count = 1
print('\nSend a KeyboardInterrupt to stop publishing')
try: 
    while True:
        while count <= MSG_COUNT:
            print(f'============================')
            print(f'\nCount: {count}')
            topic = Topic.of(TOPIC_PREFIX + '/' + REQUEST_TOPIC + '/'  + f'{count}')

            # Direct publish the message with dynamic headers and payload
            outbound_msg = outbound_msg_builder \
                                .with_application_message_id(f'\nNEW {count}')\
                                .build(f'\n{message_body} + {count}')
            print(f'>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
            print(f'Publishing request on {outbound_msg}')
            try:
                response = direct_requestor \
                                .publish_await_response(request_message=outbound_msg, \
                                                        request_destination=topic, \
                                                        reply_timeout=10000)
                print(f'<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
                print(f'Received reply:\n{response}')
                print(f'============================')
            except Exception as e:
                pass
            count += 1
            time.sleep(1)

        print(f'\n\n-=-=-=-=-=-=-=-=-=-=-=-=-=')
        print("Next Iteration")
        print(f'-=-=-=-=-=-=-=-=-=-=-=-=-=\n\n')
        count = 1
        time.sleep(2)
        
except KeyboardInterrupt:
    print('\nTerminating Requestor')
    direct_requestor.terminate()
    print('\nDisconnecting Messaging Service')
    messaging_service.disconnect()