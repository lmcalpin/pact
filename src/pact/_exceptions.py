class ActorException(Exception):
    """Generic Exception raised when the actor system can not handle a request."""
    
class ActorStoppedException(Exception):
    """A message was delivered to an actor after it was stopped."""
    
class DuplicateActorException(ActorException):
    """Raised when trying to register an actor that is already registered in the ActorSystem."""
    
class InternalException(Exception):
    """An error occurred in the actor framework that is probably the result of a bug."""
    
