# Backend/execution/base_executor.py

from abc import ABC, abstractmethod
from typing import Any


class BaseExecutor(ABC):

    @abstractmethod
    def load_df(self, file_path: str):
        pass

    @abstractmethod
    def execute_query(
        self,
        file_path: str,
        pyspark_code: str
    ) -> list[dict]:
        pass

    @abstractmethod
    def execute_profile(self, file_path: str) -> dict:
        pass

    @abstractmethod
    def execute_quality(self, file_path: str) -> dict:
        pass
