from analytics.profiling.profiling_code_generator import ProfilingCodeGenerator


class StaticCommandRouter:

    def __init__(self, question: str):
        self.question = question.strip().lower()

    def generate_pyspark(self) -> str:

        if not self.question.startswith("@"):
            raise ValueError("Invalid static command. Must start with '@'")

        if "@profiling" in self.question:
            generator = ProfilingCodeGenerator()
            return generator.generate()

        raise ValueError(f"Unsupported command: {self.question}")
