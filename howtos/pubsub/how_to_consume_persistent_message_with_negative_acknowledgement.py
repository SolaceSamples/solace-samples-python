"""sampler to consume persistent message with negative acknowledgement"""

import threading
from typing import TypeVar

from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.inbound_message import InboundMessage
from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.config.message_acknowledgement_configuration import Outcome
from howtos.pubsub.how_to_consume_persistent_message import HowToConsumeMessageExclusiveVsSharedMode
from howtos.pubsub.how_to_publish_persistent_message import HowToPublishPersistentMessage
from howtos.sampler_boot import SolaceConstants, SamplerBoot, BasicTestMessageHandler
from howtos.sampler_master import SamplerMaster

X = TypeVar('X')
constants = SolaceConstants
boot = SamplerBoot()
lock = threading.Lock()

topic_name = constants.TOPIC_ENDPOINT_DEFAULT
topic = Topic.of(topic_name)


class HowToSettlePersistentMessageWithNegativeAcknowledgement:
    """class contains methods to settle message with negative acknowledgement"""

    @staticmethod
    def settle_message_sync(service: MessagingService, publisher, message, required_outcomes, outcome_to_settle):
        try:
            queue_name = constants.QUEUE_NAME_FORMAT.substitute(iteration=topic_name)
            HowToConsumeMessageExclusiveVsSharedMode.create_queue_and_add_topic(queue_name)
            queue_to_consume = Queue.durable_exclusive_queue(queue_name)

            receiver: PersistentMessageReceiver = service.create_persistent_message_receiver_builder() \
                .with_required_message_outcome_support(*required_outcomes).build(queue_to_consume)
            receiver.start()
            print(f'PERSISTENT receiver started for sync receiver... Listening to Queue [{queue_to_consume.get_name()}]')
            receiver.add_subscription(TopicSubscription.of(topic_name))

            HowToPublishPersistentMessage.publish_string_message_non_blocking(publisher, topic, message)

            message: InboundMessage = receiver.receive_message()
            print(f"the message payload is {message.get_payload_as_string()}")
            receiver.settle(message, outcome_to_settle)
        finally:
            receiver.terminate()
            HowToConsumeMessageExclusiveVsSharedMode.delete_queue(queue_to_consume.get_name())

    @staticmethod
    def settle_message_async(service: MessagingService, publisher, message, required_outcomes, outcome_to_settle):
        event = threading.Event()
        receiver = None

        def receiver_callback(self, message: InboundMessage):
            # Fail messages will be redelivered so we just want to settle the actual message that is not redelivered
            if not message.is_redelivered():
                print(f"the message payload is {message.get_payload_as_string()}")
                receiver.settle(message, outcome_to_settle)
                event.set()

        try:
            queue_name = constants.QUEUE_NAME_FORMAT.substitute(iteration=topic_name)
            HowToConsumeMessageExclusiveVsSharedMode.create_queue_and_add_topic(queue_name)
            queue_to_consume = Queue.durable_exclusive_queue(queue_name)

            receiver: PersistentMessageReceiver = service.create_persistent_message_receiver_builder() \
                .with_required_message_outcome_support(*required_outcomes).build(queue_to_consume)
            receiver.start()
            print(f'PERSISTENT receiver started for async receiver... Listening to Queue [{queue_to_consume.get_name()}]')
            receiver.add_subscription(TopicSubscription.of(topic_name))
            message_handler = BasicTestMessageHandler(test_callback=receiver_callback)
            receiver.receive_async(message_handler)

            HowToPublishPersistentMessage.publish_string_message_non_blocking(publisher, topic, message)
            event.wait(5)
        finally:
            receiver.terminate()
            HowToConsumeMessageExclusiveVsSharedMode.delete_queue(queue_to_consume.get_name())

    @staticmethod
    def run():
        try:
            # Set up for required outcomes and outcome to settle
            required_outcomes = (Outcome.ACCEPTED, Outcome.FAILED, Outcome.REJECTED)
            outcome_to_settle =  (Outcome.ACCEPTED, Outcome.FAILED, Outcome.REJECTED)

            message = constants.MESSAGE_TO_SEND
            messaging_service = SamplerMaster.connect_messaging_service()

            publisher = HowToPublishPersistentMessage.create_persistent_message_publisher(messaging_service)

            for outcome in outcome_to_settle:
                HowToSettlePersistentMessageWithNegativeAcknowledgement \
                    .settle_message_sync(service=messaging_service, publisher=publisher, message=message, 
                                        required_outcomes=required_outcomes, outcome_to_settle=outcome)

                HowToSettlePersistentMessageWithNegativeAcknowledgement \
                    .settle_message_async(service=messaging_service, publisher=publisher, message=message, 
                                        required_outcomes=required_outcomes, outcome_to_settle=outcome)
        finally:
            messaging_service.disconnect()


if __name__ == '__main__':
    HowToSettlePersistentMessageWithNegativeAcknowledgement.run()
