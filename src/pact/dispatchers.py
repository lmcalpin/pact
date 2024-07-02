from pact._base import ActorRegistry, ActorRef
from pact.messages import Envelope, Reply
from concurrent.futures import Future
from pact.exceptions import *
import queue
import logging
from abc import ABC
from typing import Any
import boto3 # type: ignore[import-untyped]

LOGGER = logging.getLogger(__name__)

class Dispatcher(ABC):
    def tell(self, envelope: Envelope):
        raise NotImplementedError
        
    def ask(self, envelope: Envelope) -> Future:
        raise NotImplementedError

class InMemDispatcher(Dispatcher):
    def __init__(self, actor_system: ActorRegistry):
        self.messages_in_transit = queue.SimpleQueue[Envelope]()
        self.awaiting_reply: dict[str, Future] = {}
        self.actor_system = actor_system
    
    def tell(self, envelope: Envelope):
        self.messages_in_transit.put(envelope)
        LOGGER.debug(f'  -- sending {envelope.message_id} to {envelope.recipient_id} with {envelope.message}')
        
    def ask(self, envelope: Envelope):
        self.tell(envelope)
        future_reply = Future[object]()
        self.awaiting_reply[envelope.message_id] = future_reply
        LOGGER.debug(f'  -- sending {envelope.message_id} to {envelope.recipient_id} with {envelope.message} --- awaiting response')
        return future_reply
        
    def _distribute_envelopes(self):
        """
        Takes messages from the Dispatcher's Queue and moves them into the Mailbox for the Actor
        that is the intended recipient.
        """
        while True:
            try:
                LOGGER.debug('looking for more messages')
                envelope = self.messages_in_transit.get(timeout=1)
                LOGGER.debug(f' -- found {envelope}')
                if envelope:
                    if isinstance(envelope, Reply):
                        future_reply = self.awaiting_reply[envelope.recipient_id]
                        future_reply.set_result(envelope.message)
                        print(future_reply)
                        LOGGER.debug(f'  -- replied to {envelope.recipient_id} with {envelope.message}')
                    else:
                        actor_ref : ActorRef = self.actor_system.locate(envelope.recipient_id)
                        if not actor_ref:
                            raise InternalException()
                        actor_ref._actor._mailbox.deliver(envelope)
            except queue.Empty:
                return
            # when there are no more messages to read, return
            if self.messages_in_transit.empty():
                return
            
class SqsDispatcher(Dispatcher):
    def __init__(self, actor_system: ActorRegistry, sqs_queue_name: str):
        self.actor_system = actor_system
        # Boto3 SQS client
        self.sqs = boto3.client('sqs')
        # Get the queue URL by name
        self.queue_url = self.sqs.get_queue_url(QueueName=sqs_queue_name)['QueueUrl']
        self.awaiting_reply: dict[str, Future] = {}

    def tell(self, envelope: Envelope):
        # Send message to SQS queue
        message_body = self._serialize_envelope(envelope)
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=message_body)
        LOGGER.debug(f'  -- sending {envelope.message_id} to {envelope.recipient_id} with {envelope.message}')
        
    def ask(self, envelope: Envelope) -> Future:
        self.tell(envelope)
        future_reply = Future[Any]()
        self.awaiting_reply[envelope.message_id] = future_reply
        LOGGER.debug(f'  -- sending {envelope.message_id} to {envelope.recipient_id} with {envelope.message} --- awaiting response')
        return future_reply
        
    def _distribute_envelopes(self):
        """
        Retrieves messages from the SQS queue and moves them into the Mailbox
        for the Actor that is the intended recipient.
        """
        while True:
            try:
                LOGGER.debug('looking for more messages')
                # Receive messages from SQS queue
                messages = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=10,  # Max number of messages to fetch
                    WaitTimeSeconds=1        # Short polling duration
                ).get('Messages', [])
                
                if not messages:
                    break

                for message in messages:
                    envelope = self._deserialize_envelope(message['Body'])
                    LOGGER.debug(f' -- found {envelope}')
                    if envelope:
                        if isinstance(envelope, Reply):
                            future_reply = self.awaiting_reply.pop(envelope.message_id, None)
                            if future_reply:
                                future_reply.set_result(envelope.message)
                                LOGGER.debug(f'  -- replied to {envelope.recipient_id} with {envelope.message}')
                        else:
                            actor_ref = self.actor_system.locate(envelope.recipient_id)
                            if not actor_ref:
                                raise InternalException()
                            actor_ref._actor._mailbox.deliver(envelope)
                    # Remove the message from SQS queue after processing
                    self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message['ReceiptHandle'])
                
            except Exception as e:
                LOGGER.error('Error distributing envelopes: ', exc_info=e)
                break

    def _serialize_envelope(self, envelope: Envelope) -> str:
        # Serialize an Envelope object to a string
        return f"{envelope.message_id}|{envelope.recipient_id}|{envelope.expects_reply}|{envelope.message}"

    def _deserialize_envelope(self, message_body: str) -> Envelope:
        # Deserialize a string to an Envelope object
        parts = message_body.split('|', 3)
        message_id, recipient_id, expects_reply, message = parts
        return Envelope(message, recipient_id, expects_reply.lower() == 'true')
