from concurrent.futures import Future
from pact._exceptions import *
import queue
import uuid
import threading
import logging

LOGGER = logging.getLogger(__name__)

class ActorRef():
    def name(self) -> str:
        raise NotImplementedError()

    def tell(self, message):
        raise NotImplementedError()

    def ask(self, message) -> Future:
        raise NotImplementedError()

class Envelope():
    """
    An Envelope wraps a message with additional metadata.
    """
    def __init__(self, message, recipient_id: str, expects_reply: bool = False):
        self.message_id = str(uuid.uuid4())
        self.message = message
        self.recipient_id = recipient_id
        self.expects_reply = expects_reply
        
class Reply(Envelope):
    """
    A Reply wraps the response to the invocation of the ask method on an ActorRef with additional metadata.
    """
    def __init__(self, reply, reply_to_message_id: str):
        super().__init__(reply, reply_to_message_id, expects_reply=False)
        
class Actor():
    """
    An actor is a computational model where the processing units (the actors) interact with
    other actors only by sending messages.

    Args:
        :param message: a message that has been sent to this Actor for processing
    """
    def __init__(self):
        self._started = True
        self._busy = False
        self._mailbox = Mailbox()
        
    def on_message(self, message: object):
        raise NotImplementedError()
    
    def _deliver_message(self, message):
        if not self._busy:
            try:
                self._busy = True
                return self.on_message(message)
            finally:
                self._busy = False
        LOGGER.error(f'Actor received a message while processing')
        raise InternalException()
    
    def is_running(self) -> bool:
        return self._started
    
    def has_mail(self) -> bool:
        return self._mailbox.has_mail()
    
    def start(self):
        pass
    
    def stop(self):
        self._busy = True
        self._started = False
        

class Mailbox():
    def __init__(self):
        self.inbox = queue.SimpleQueue()
    
    def deliver(self, message : Envelope):
        return self.inbox.put(message)
    
    def take(self) -> object:
        try:
            return self.inbox.get(block=False)
        except queue.Empty:
            return None
    
    def has_mail(self):
        return not self.inbox.empty()

class ActorSystem():
    def __init__(self):
        self.actor_refs = {}
        self.dispatcher = Dispatcher(self)
        self.stopped = False
        self.thread = threading.Thread(target=self.__actor_processing_loop)
        self.thread.start()
    
    def register(self, actor: Actor, name: str | None = None):
        if not name:
            name = str(uuid.uuid4)
        if self.actor_refs.get(name):
            raise DuplicateActorException(f'Actor {name} is already registered')
        actor_ref = self._bind(name, actor)
        self.actor_refs[name] = actor_ref
        return actor_ref
    
    def locate(self, name: str) -> ActorRef:
        return self.actor_refs[name]
    
    def _bind(self, name: str, actor: Actor):
        return LocalActorRef(self, name, actor)
    
    def __actor_processing_loop(self):
        while not self.stopped:
            LOGGER.debug('start of actor loop')
            all_actor_refs = [ar for ar in self.actor_refs.values() if ar._actor.has_mail]
            for ar in all_actor_refs:
                LOGGER.debug(f'retrieving messages for actor {ar._name} {ar._actor._mailbox.inbox.qsize()}')
                self._retrieve_messages(ar._actor)
            self.dispatcher._distribute_envelopes()
            LOGGER.debug('end of actor loop')
        LOGGER.debug('Actor system shut down')
        
    def _retrieve_messages(self, actor: Actor):
        while actor.has_mail():
            envelope = actor._mailbox.take()
            if envelope:
                LOGGER.debug(f'Delivering message {envelope}')
                reply = actor._deliver_message(envelope.message)
                if envelope.expects_reply:
                    self.dispatcher.tell(Reply(reply, envelope.message_id))
    
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stopped = True
        self.thread.join()
            
class LocalActorRef(ActorRef):
    def __init__(self, actor_system: ActorSystem, name: str, actor: Actor):
        self.actor_system = actor_system
        self._actor = actor
        self._name = name
        
    def tell(self, message : object):
        LOGGER.debug(f'Sending {message}')
        envelope = Envelope(message, self._name)
        self.actor_system.dispatcher.tell(envelope)
    
    def ask(self, message : object) -> Future:
        envelope = Envelope(message, self._name, expects_reply=True)
        return self.actor_system.dispatcher.ask(envelope)

class Dispatcher():
    def __init__(self, actor_system: ActorSystem):
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
                    # 
                    if isinstance(envelope, Reply):
                        future_reply = self.awaiting_reply[envelope.recipient_id]
                        future_reply.set_result(envelope.message)
                        print(future_reply)
                        LOGGER.debug(f'  -- replied to {envelope.recipient_id} with {envelope.message}')
                    else:
                        actor_ref = self.actor_system.locate(envelope.recipient_id)
                        if not actor_ref:
                            raise InternalException()
                        actor_ref._actor._mailbox.deliver(envelope)
            except queue.Empty:
                return
            # when there are no more messages to read, return
            if self.messages_in_transit.empty():
                return
