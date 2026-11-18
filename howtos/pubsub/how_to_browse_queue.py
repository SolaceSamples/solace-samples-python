"""sampler for browsing messages on a queue without consuming them"""

import time
import threading
from typing import TypeVar

from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.inbound_message import InboundMessage
from solace.messaging.receiver.queue_browser import MessageQueueBrowser
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.config.solace_properties.message_properties import CORRELATION_ID, PRIORITY
from howtos.pubsub.how_to_consume_persistent_message import HowToConsumeMessageExclusiveVsSharedMode
from howtos.pubsub.how_to_publish_persistent_message import HowToPublishPersistentMessage
from howtos.sampler_boot import SolaceConstants, SamplerBoot
from howtos.sampler_master import SamplerMaster

X = TypeVar('X')
constants = SolaceConstants
boot = SamplerBoot()
lock = threading.Lock()

topic_name = constants.TOPIC_ENDPOINT_DEFAULT
topic = Topic.of(topic_name)


class HowToBrowseQueue:
    """class contains methods demonstrating how to browse messages on a queue"""

    @staticmethod
    def browse_messages_on_queue(messaging_service: MessagingService, queue_to_browse: Queue,
                                 publisher, num_messages: int = 3):
        """method to browse messages on a queue without consuming them

        Creates a MessageQueueBrowser, publishes messages to the queue, then browses
        them. Browsed messages remain on the queue and are still available for consumption.

        Args:
            messaging_service: connected messaging service
            queue_to_browse: the queue to browse messages from
            publisher: persistent message publisher to publish test messages
            num_messages: number of messages to publish and browse
        """
        browser = None
        try:
            browser: MessageQueueBrowser = messaging_service.create_message_queue_browser_builder() \
                .build(queue_to_browse)
            browser.start()
            print(f'Queue browser started... Browsing Queue [{queue_to_browse.get_name()}]')

            for i in range(num_messages):
                HowToPublishPersistentMessage.publish_string_message_non_blocking(
                    publisher, topic, f'{constants.MESSAGE_TO_SEND} - msg {i}')
            time.sleep(1)

            browse_count = 0
            while True:
                message: InboundMessage = browser.receive_message(timeout=1000)
                if message is None:
                    break
                print(f'Browsed message [{browse_count}]: {message.get_payload_as_string()} '
                      f'from topic: {message.get_destination_name()}')
                browse_count += 1

            print(f'Total messages browsed: {browse_count}')
        finally:
            if browser is not None:
                browser.terminate()

    @staticmethod
    def browse_messages_with_selector(messaging_service: MessagingService, queue_to_browse: Queue, publisher):
        """method to browse messages on a queue using a message selector

        Only messages matching the selector expression will be returned by the browser.

        Args:
            messaging_service: connected messaging service
            queue_to_browse: the queue to browse messages from
            publisher: persistent message publisher to publish test messages
        """
        browser = None
        try:
            selector_expression = "JMSCorrelationID = 'browse-me'"

            browser: MessageQueueBrowser = messaging_service.create_message_queue_browser_builder() \
                .with_message_selector(selector_expression) \
                .build(queue_to_browse)
            browser.start()
            print(f'Queue browser with selector started... Browsing Queue [{queue_to_browse.get_name()}]')
            print(f'Selector: {selector_expression}')

            outbound_msg = messaging_service.message_builder() \
                .with_application_message_id(constants.APPLICATION_MESSAGE_ID)

            matching_props = {CORRELATION_ID: 'browse-me', PRIORITY: 1}
            HowToPublishPersistentMessage \
                .publish_typed_message_with_extended_message_props_non_blocking(
                    outbound_msg, publisher, topic, 'matching message', matching_props)

            non_matching_props = {CORRELATION_ID: 'skip-me', PRIORITY: 1}
            HowToPublishPersistentMessage \
                .publish_typed_message_with_extended_message_props_non_blocking(
                    outbound_msg, publisher, topic, 'non-matching message', non_matching_props)
            time.sleep(1)

            browse_count = 0
            while True:
                message: InboundMessage = browser.receive_message(timeout=1000)
                if message is None:
                    break
                print(f'Browsed (selector-matched) message: {message.get_payload_as_string()}')
                browse_count += 1

            print(f'Messages browsed with selector: {browse_count}')
        finally:
            if browser is not None:
                browser.terminate()

    @staticmethod
    def browse_and_remove_message(messaging_service: MessagingService, queue_to_browse: Queue, publisher):
        """method to browse messages and selectively remove one from the queue

        Messages removed via the browser are deleted from the queue and will
        no longer be available for consumption by persistent receivers.

        Args:
            messaging_service: connected messaging service
            queue_to_browse: the queue to browse messages from
            publisher: persistent message publisher to publish test messages
        """
        browser = None
        try:
            browser: MessageQueueBrowser = messaging_service.create_message_queue_browser_builder() \
                .build(queue_to_browse)
            browser.start()
            print(f'Queue browser started for browse-and-remove... Browsing Queue [{queue_to_browse.get_name()}]')

            HowToPublishPersistentMessage.publish_string_message_non_blocking(
                publisher, topic, 'message to keep')
            HowToPublishPersistentMessage.publish_string_message_non_blocking(
                publisher, topic, 'message to remove')
            HowToPublishPersistentMessage.publish_string_message_non_blocking(
                publisher, topic, 'another message to keep')
            time.sleep(1)

            browse_count = 0
            removed_count = 0
            while True:
                message: InboundMessage = browser.receive_message(timeout=1000)
                if message is None:
                    break
                payload = message.get_payload_as_string()
                browse_count += 1
                if 'remove' in payload:
                    browser.remove(message)
                    removed_count += 1
                    print(f'Removed message: {payload}')
                else:
                    print(f'Kept message: {payload}')

            print(f'Total browsed: {browse_count}, Removed: {removed_count}')
        finally:
            if browser is not None:
                browser.terminate()

    @staticmethod
    def browse_with_custom_window_size(messaging_service: MessagingService, queue_to_browse: Queue,
                                       publisher, window_size: int = 10):
        """method to browse messages with a custom browser window size

        The window size controls the maximum number of messages that can be
        pre-fetched by the browser. Valid range is 1-255, default is 255.

        Args:
            messaging_service: connected messaging service
            queue_to_browse: the queue to browse messages from
            publisher: persistent message publisher to publish test messages
            window_size: the browser window size (1-255)
        """
        browser = None
        try:
            browser: MessageQueueBrowser = messaging_service.create_message_queue_browser_builder() \
                .with_queue_browser_window_size(window_size) \
                .build(queue_to_browse)
            browser.start()
            print(f'Queue browser started with window size {window_size}... '
                  f'Browsing Queue [{queue_to_browse.get_name()}]')

            for i in range(5):
                HowToPublishPersistentMessage.publish_string_message_non_blocking(
                    publisher, topic, f'{constants.MESSAGE_TO_SEND} - windowed msg {i}')
            time.sleep(1)

            browse_count = 0
            while True:
                message: InboundMessage = browser.receive_message(timeout=1000)
                if message is None:
                    break
                print(f'Browsed message [{browse_count}]: {message.get_payload_as_string()}')
                browse_count += 1

            print(f'Total messages browsed with window size {window_size}: {browse_count}')
        finally:
            if browser is not None:
                browser.terminate()

    @staticmethod
    def run():
        try:
            messaging_service = SamplerMaster.connect_messaging_service()
            publisher = HowToPublishPersistentMessage.create_persistent_message_publisher(messaging_service)

            queue_name = constants.QUEUE_NAME_FORMAT.substitute(iteration='browse-' + topic_name)
            HowToConsumeMessageExclusiveVsSharedMode.create_queue_and_add_topic(queue_name)
            queue_to_browse = Queue.durable_exclusive_queue(queue_name)

            print('\n=== Browse messages on a queue ===')
            HowToBrowseQueue.browse_messages_on_queue(messaging_service, queue_to_browse, publisher)

            print('\n=== Browse messages with a selector ===')
            HowToBrowseQueue.browse_messages_with_selector(messaging_service, queue_to_browse, publisher)

            print('\n=== Browse and selectively remove messages ===')
            HowToBrowseQueue.browse_and_remove_message(messaging_service, queue_to_browse, publisher)

            print('\n=== Browse with custom window size ===')
            HowToBrowseQueue.browse_with_custom_window_size(messaging_service, queue_to_browse, publisher,
                                                            window_size=10)
        finally:
            publisher.terminate()
            HowToConsumeMessageExclusiveVsSharedMode.delete_queue(queue_name)
            messaging_service.disconnect()


if __name__ == '__main__':
    HowToBrowseQueue.run()
