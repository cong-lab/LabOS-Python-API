"""Helpers for turning Python callables into remote tool definitions."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, get_args, get_origin, get_type_hints

from pydantic import TypeAdapter, create_model
from pydantic.fields import FieldInfo

from labos.mcp.context import Context
from labos.mcp.protocol import ToolDefinition


def build_tool_definition(
    fn: Callable[..., Any],
    *,
    name: str | None = None,
    description: str | None = None,
    risk: str = "low",
    user_confirmation: bool = False,
) -> ToolDefinition:
    signature = inspect.signature(fn)
    type_hints = get_type_hints(fn)
    input_model = build_input_model(fn)
    return_annotation = type_hints.get("return", signature.return_annotation)
    output_schema = None
    if return_annotation is not inspect.Signature.empty:
        output_schema = TypeAdapter(return_annotation).json_schema()

    return ToolDefinition(
        name=name or fn.__name__,
        description=description or inspect.getdoc(fn) or "",
        input_schema=input_model.model_json_schema(),
        output_schema=output_schema,
        risk=risk,  # type: ignore[arg-type]
        user_confirmation=user_confirmation,
    )


def build_input_model(fn: Callable[..., Any]) -> type:
    signature = inspect.signature(fn)
    type_hints = get_type_hints(fn)
    fields: dict[str, tuple[Any, Any]] = {}

    for param_name, parameter in signature.parameters.items():
        annotation = type_hints.get(param_name, parameter.annotation)
        if _is_context_annotation(annotation):
            continue

        if annotation is inspect.Parameter.empty:
            annotation = Any

        default = ... if parameter.default is inspect.Parameter.empty else parameter.default
        fields[param_name] = (annotation, default)

    return create_model(f"{fn.__name__.title().replace('_', '')}Input", **fields)


def get_context_parameter(fn: Callable[..., Any]) -> str | None:
    signature = inspect.signature(fn)
    type_hints = get_type_hints(fn)

    for param_name, parameter in signature.parameters.items():
        annotation = type_hints.get(param_name, parameter.annotation)
        if _is_context_annotation(annotation):
            return param_name

    return None


def _is_context_annotation(annotation: Any) -> bool:
    if annotation is Context:
        return True

    if isinstance(annotation, FieldInfo):
        annotation = annotation.annotation

    return any(arg is Context for arg in get_args(annotation)) or get_origin(annotation) is Context
