from pact.actor import Mailbox, Envelope

def test_mailbox():
    mailbox = Mailbox()
    assert not mailbox.has_mail()
    mailbox.deliver(Envelope('hi', 'recipient1'))
    envelope = mailbox.take()
    assert envelope.message == 'hi'