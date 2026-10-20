"""sampler to configure the maximum number of unacknowledged messages for a persistent receiver"""

from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.inbound_message import InboundMessage
from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.resources.queue import Queue
from solace.messaging.config.solace_properties.receiver_properties import PERSISTENT_MAX_UNACKED_MESSAGES
from howtos.sampler_master import SamplerMaster


class HowToConfigureReceiverMaxUnackedMessages:
    """class contains methods to configure max unacknowledged messages on a persistent receiver

    The max unacked messages property controls how many messages the broker delivers to the
    receiver before it must acknowledge them. This is useful for flow control — limiting
    unacknowledged messages prevents the receiver from being overwhelmed.
    """

    @staticmethod
    def using_builder_method(service: MessagingService, queue_to_consume: Queue, max_unacked_messages: int):
        """Configure max unacked messages using the fluent builder method"""
        receiver = None
        try:
            receiver: PersistentMessageReceiver = service.create_persistent_message_receiver_builder() \
                .with_max_unacked_messages(max_unacked_messages) \
                .build(queue_to_consume)
            receiver.start()
            message: InboundMessage = receiver.receive_message(timeout=5000)
            if message:
                print(f"the message payload is {message.get_payload_as_string()}")
                receiver.ack(message)
        finally:
            if receiver:
                receiver.terminate()

    @staticmethod
    def using_properties_map(service: MessagingService, queue_to_consume: Queue, max_unacked_messages: int):
        """Configure max unacked messages using the from_properties builder method"""
        receiver = None
        try:
            receiver: PersistentMessageReceiver = service.create_persistent_message_receiver_builder() \
                .from_properties({PERSISTENT_MAX_UNACKED_MESSAGES: max_unacked_messages}) \
                .build(queue_to_consume)
            receiver.start()
            message: InboundMessage = receiver.receive_message(timeout=5000)
            if message:
                print(f"the message payload is {message.get_payload_as_string()}")
                receiver.ack(message)
        finally:
            if receiver:
                receiver.terminate()

    @staticmethod
    def run():
        messaging_service = None
        try:
            queue_name = 'Q/test/max_unacked'
            max_unacked_messages = 10
            messaging_service = SamplerMaster.connect_messaging_service()
            durable_exclusive_queue = Queue.durable_exclusive_queue(queue_name)
            HowToConfigureReceiverMaxUnackedMessages \
                .using_builder_method(service=messaging_service, queue_to_consume=durable_exclusive_queue,
                                      max_unacked_messages=max_unacked_messages)
            HowToConfigureReceiverMaxUnackedMessages \
                .using_properties_map(service=messaging_service, queue_to_consume=durable_exclusive_queue,
                                      max_unacked_messages=max_unacked_messages)
        finally:
            if messaging_service:
                messaging_service.disconnect()


if __name__ == '__main__':
    HowToConfigureReceiverMaxUnackedMessages.run()
