import uuid
import queue

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

