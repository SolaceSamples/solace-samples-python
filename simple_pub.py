import os

# Import Solace Python  API modules from the pysolace package
from pysolace.messaging.messaging_service import MessagingService
from pysolace.messaging.utils.topic import Topic

def direct_message_publish(messaging_service: MessagingService, topic, message):
    """ to publish str or byte array type message"""

    try:
        direct_publish_service = messaging_service.create_direct_message_publisher_builder().build()
        direct_publish_service.start_async()
        direct_publish_service.publish(destination=topic, message=message)
    finally:
        direct_publish_service.terminate()

# Broker Config
broker_props = {
    "solace.messaging.transport.host": os.environ['HOST'],
    "solace.messaging.service.vpn-name": os.environ['VPN'],
    "solace.messaging.authentication.scheme.basic.user-name": os.environ['SOL_USERNAME'],
    "solace.messaging.authentication.scheme.basic.password": os.environ['SOL_PASSWORD']
    }


# Initialize A messaging service
messaging_service = MessagingService.builder().from_properties(broker_props).build()
messaging_service.connect_async()

# Set a destination Topic and message body
topic = Topic.of("tamimi/v1/hey")
body = "this is the body of the msg"

# Direct publish the message
direct_message_publish(messaging_service, topic, body)