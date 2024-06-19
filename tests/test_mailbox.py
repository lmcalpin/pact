from pact.actor import Mailbox, Envelope

class TestMailbox():
    def test_mailbox(self):
        mailbox = Mailbox()
        assert not mailbox.has_mail()
        mailbox.deliver(Envelope('hi', 'recipient1'))
        envelope = mailbox.take()
        assert envelope.message == 'hi'