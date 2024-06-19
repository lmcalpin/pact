from pact import ActorSystem, Actor
from threading import Event

class TestActors():
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
            fake_actor = EchoActor()
            ref = s.register(fake_actor, name='fake')
            f = ref.ask('hi')
            assert 'hi' == f.result()
