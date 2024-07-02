from concurrent.futures import Future
from abc import ABC, abstractmethod

class ActorRef(ABC):
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def tell(self, message):
        raise NotImplementedError

    @abstractmethod
    def ask(self, message) -> Future:
        raise NotImplementedError

class ActorRegistry(ABC):
    @abstractmethod
    def locate(self, name: str) -> ActorRef:
        raise NotImplementedError
        
