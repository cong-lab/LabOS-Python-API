from typing import Literal

from labos.mcp import Context
from labos.mcp.schema import build_tool_definition


def test_build_tool_definition_uses_signature_docstring_and_type_hints() -> None:
    def read_temperature(units: Literal["C", "F"], retries: int = 1, ctx: Context | None = None) -> float:
        """Read the current temperature."""
        return 21.5

    tool = build_tool_definition(read_temperature)

    assert tool.name == "read_temperature"
    assert tool.description == "Read the current temperature."
    assert tool.input_schema["type"] == "object"
    assert tool.input_schema["properties"]["units"]["enum"] == ["C", "F"]
    assert tool.input_schema["properties"]["retries"]["default"] == 1
    assert "ctx" not in tool.input_schema["properties"]
    assert tool.input_schema["required"] == ["units"]
    assert tool.output_schema["type"] == "number"
