def strip_code_fences(code: str) -> str:
    """
    Remove markdown ``` wrappers from LLM output.
    """
    code = code.strip()

    if code.startswith("```"):
        code = code.split("```", 1)[1]

    if code.endswith("```"):
        code = code.rsplit("```", 1)[0]

    return code.strip()
