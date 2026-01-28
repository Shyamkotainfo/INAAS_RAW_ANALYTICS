# Backend/bedrock/nova_client.py

class NovaClient:
    def generate(self, prompt: str, question: str) -> str:
        q = question.lower()

        # list / show / get all employees
        if any(x in q for x in ["list", "show", "get"]) and "employee" in q:
            return """
result_df = df
""".strip()

        # count employees
        if "count" in q and "employee" in q:
            return """
result_df = df.select("EmpCode").distinct().count()
""".strip()

        # group by
        if "group" in q or "department" in q:
            return """
result_df = df.groupBy("Designation").count()
""".strip()

        # default
        return """
result_df = df.limit(10)
""".strip()
