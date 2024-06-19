from pact import ActorSystem, Actor, InternalException
from threading import Event
import pytest
from unittest.mock import Mock, patch

class DummyActor(Actor):
    def on_message(self, message):
        return message

@pytest.fixture
def actor():
    return DummyActor()

class TestActors:
    def test_tell(self):
        class FakeActor(Actor):
            def __init__(self, event: Event):
                super().__init__()
                self._last_message = None
                self.event = event
                
            def on_message(self, msg: str):
                self._last_message = msg
                self.event.set()
                return f'{msg} {self._last_message}'

        with ActorSystem() as s:
            event = Event()
            fake_actor = FakeActor(event)
            ref = s.register(fake_actor, name='fake')
            assert fake_actor._last_message is None
            ref.tell('hi')
            event.wait(1)
            event.clear()
            assert fake_actor._last_message == 'hi'

    def test_ask(self):
        class EchoActor(Actor):
            def on_message(self, msg: str):
                return msg

        with ActorSystem() as s:
            echo_actor = EchoActor()
            ref = s.register(echo_actor, name='echo')
            f = ref.ask('hi')
            assert 'hi' == f.result()

    def test_on_message_not_implemented(self):
        actor = Actor()
        with pytest.raises(NotImplementedError):
            actor.on_message(None)

    def test_deliver_message_when_not_busy(self, actor):
        message = "test message"
        with patch.object(actor, 'on_message', return_value=None) as mock_on_message:
            actor._deliver_message(message)
            mock_on_message.assert_called_once_with(message)

    def test_deliver_message_when_busy(self, actor):
        actor._busy = True
        with pytest.raises(InternalException):
            actor._deliver_message("test message")

    def test_is_running(self, actor):
        assert actor.is_running() is True

    def test_has_mail(self, actor):
        actor._mailbox = Mock()
        actor._mailbox.has_mail.return_value = True
        assert actor.has_mail() is True

    def test_stop(self, actor):
        actor.stop()
        assert actor._busy is True
        assert actor._started is False
