from analytics.profiling.profiling_code_generator import ProfilingCodeGenerator
from analytics.quality.quality_code_generator import QualityCodeGenerator


class StaticCommandRouter:
    """
    Handles all @ static commands.

    Supported:
        @profiling
        @profile
        @quality
    """

    def __init__(self, question: str):
        self.question = question.strip().lower()

    def generate_pyspark(self) -> str:

        if not self.question.startswith("@"):
            raise ValueError("Invalid static command. Must start with '@'")

        # -----------------------------------------
        # PROFILING
        # -----------------------------------------
        if "profiling" in self.question or "profile" in self.question:
            generator = ProfilingCodeGenerator()
            return generator.generate()

        # -----------------------------------------
        # QUALITY
        # -----------------------------------------
        if "quality" in self.question:
            generator = QualityCodeGenerator()
            return generator.generate()

        # -----------------------------------------
        # UNKNOWN COMMAND
        # -----------------------------------------
        raise ValueError(f"Unsupported command: {self.question}")
