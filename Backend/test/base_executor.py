from abc import ABC, abstractmethod


class BaseExecutor(ABC):
    @abstractmethod
    def execute(self, context: dict, pyspark_code: str):
        pass
