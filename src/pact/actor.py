from pact._base import ActorRef, ActorRegistry
from pact.messages import Envelope, Reply, Mailbox
from pact.exceptions import *
from pact.dispatchers import Dispatcher, InMemDispatcher
from concurrent.futures import Future
import uuid
import threading
import logging
from typing import Any, Optional, override

LOGGER = logging.getLogger(__name__)

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
        
    def on_message(self, message: Any):
        raise NotImplementedError()
    
    def _deliver_message(self, message: Any):
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
        

class ActorSystem(ActorRegistry):
    def __init__(self, dispatcher : Optional[Dispatcher] = None):
        self.actor_refs : dict[str, ActorRef] = {}
        self.dispatcher = dispatcher if dispatcher else InMemDispatcher(self)
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
        self._actor_system = actor_system
        self._actor = actor
        self._name = name
    
    @override
    def name(self):
        return self._name
    
    @override
    def tell(self, message : object):
        LOGGER.debug(f'Sending {message}')
        envelope = Envelope(message, self._name)
        self._actor_system.dispatcher.tell(envelope)
    
    @override
    def ask(self, message : object) -> Future:
        envelope = Envelope(message, self._name, expects_reply=True)
        return self._actor_system.dispatcher.ask(envelope)
