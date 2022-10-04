"""sampler for using local transactions"""

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
from solace.messaging.errors.pubsubplus_client_error import UnknownTransactionStateError, TransactionRollbackError
from solace.messaging.config.solace_properties import service_properties
from solace.messaging.resources.queue import Queue
from howtos.sampler_boot import SolaceConstants, SamplerBoot
from howtos.SEMPv2.semp_client import SempClient
from howtos.SEMPv2.semp_utility import SempUtility


constants = SolaceConstants
boot = SamplerBoot()
broker_props = boot.broker_properties()
semp_config = boot.read_semp_configuration()
semp_obj = SempClient(semp_base_url=semp_config[SamplerBoot.semp_hostname_key],
                      user_name=semp_config[SamplerBoot.semp_username_key],
                      password=semp_config[SamplerBoot.semp_password_key])
semp = SempUtility(semp_obj)

class HowtoForLocalTransactions:
    """ Collection of static methods demonstrating the local transaction API. """

    @staticmethod
    def publish_to_topic(service, topic):
        """ The simplest transaction: publish a single message and commit it.
            Returns True for success, False for failure, None in the extremely rare case
            when a network outage just at the right time prevents the broker from reporting the outcome."""
        transactional_service = service.create_transactional_service_builder().build().connect()
        publisher = transactional_service.create_transactional_message_publisher_builder().build().start()
        message = service.message_builder().build(f"Message for topic {topic}")
        publisher.publish(message, topic)
        try:
            transactional_service.commit()
        except TransactionRollbackError:
            # Commit failed, message not published.
            return False
        except UnknownTransactionStateError:
            # Commit outcome unknown, message may or may not have been published. This should be very rare.
            return None
        finally:
            transactional_service.disconnect()
        return True


    @staticmethod
    def publish_to_topics(service, topics, count):
        """ Publish <count> messages to each topic. One transaction per topic:
            Either all messages make it to a topic, or none.
            Returns the messages actually delivered. """

        messages_delivered = []
        transactional_service = service.create_transactional_service_builder().build().connect()
        publisher = transactional_service.create_transactional_message_publisher_builder().build().start()
        for topic in topics:
            try:
                messages_in_this_transaction = []
                for i in range(count):
                    message = service.message_builder().build(f"Message #{i} for topic {topic.get_name()}")
                    publisher.publish(message, topic)
                    messages_in_this_transaction.append(message)
                # Publish all messages to a topic, or none at all:
                transactional_service.commit()
                # Can't be sure messages were actually published until the commit succeeds.
                messages_delivered.extend(messages_in_this_transaction)
            except TransactionRollbackError:
                # Commit failed.
                pass
            except UnknownTransactionStateError:
                # Commit outcome unknown. This should be very rare.
                pass

        transactional_service.disconnect()
        return messages_delivered


    @staticmethod
    def receive_from_queues(service, queues, count):
        """ Receive a message from each queue in a transaction, <count> times.
            Each count is a separate transaction: A message is taken from every queue or none. """

        messages = []
        receivers = []
        transactional_service = service.create_transactional_service_builder().build().connect()
        # Start a receiver for every queue
        for queue in queues:
            receiver = transactional_service.create_transactional_message_receiver_builder().build(queue).start()
            receivers.append(receiver)

        for _ in range(count):
            # Grab a message from each receiver/queue
            messages_in_this_transaction = []
            for receiver in receivers:
                message = receiver.receive_message()
                if message is None:
                    if messages_in_this_transaction:
                        # One of the queues ran empty, rolling back the partial transaction:
                        transactional_service.rollback()
                    break
                messages_in_this_transaction.append(message)
            else: # no break
                try:
                    # Consume a message from all queues, or from none:
                    transactional_service.commit()
                    messages.extend(messages_in_this_transaction)
                except TransactionRollbackError:
                    # Commit failed.
                    pass
                except UnknownTransactionStateError:
                    # Commit outcome unknown. This should be very rare.
                    pass
        transactional_service.disconnect()
        return messages


    @staticmethod
    def run():
        """ Execute the demonstrator methods. """
        try:
            number_of_queues = 5
            messages_per_queue = 3
            messaging_service = MessagingService.builder().from_properties(boot.broker_properties()).build()
            messaging_service.connect()
            print(f'Message service is connected? {messaging_service.is_connected}')
            topics = []
            queues = []
            for i in range(number_of_queues):
                topic_name = constants.TOPIC_ENDPOINT + "/local_transaction_sample/" + str(i)
                topic = Topic.of(topic_name)
                topics.append(topic)
                queue_name = constants.QUEUE_NAME_FORMAT.substitute(iteration=topic_name)
                semp.create_queue(name=queue_name,
                                  msg_vpn_name=broker_props[service_properties.VPN_NAME],
                                  access_type="exclusive",
                                  egress_enabled=True)
                semp.add_topic_to_queue(topic_name=topic_name,
                                        queue_name=queue_name,
                                        msg_vpn_name=broker_props[service_properties.VPN_NAME])
                queue = Queue.durable_exclusive_queue(queue_name)
                queues.append(queue)

            # Publish the expected number of messages to the queues
            messages_delivered_to_topics = HowtoForLocalTransactions.publish_to_topics(messaging_service, topics, messages_per_queue)
            print("Messages published:")
            for message in messages_delivered_to_topics:
                print(message)
            # One extra to mess things up
            HowtoForLocalTransactions.publish_to_topic(messaging_service, topics[-1])
            # Consume them
            consumed_messages = HowtoForLocalTransactions.receive_from_queues(messaging_service, queues, messages_per_queue)
            print("Messages consumed:")
            #print(roundtrip_messages)
            for message in roundtrip_messages:
                print(message)


        finally:
            messaging_service.disconnect()


if __name__ == '__main__':
    HowtoForLocalTransactions.run()
