"""Exceptions raised by the LabOS remote MCP client."""


class LabOSMCPError(Exception):
    """Base exception for LabOS MCP errors."""


class ToolNotFoundError(LabOSMCPError):
    """Raised when a remote call references an unknown local tool."""


class ToolExecutionError(LabOSMCPError):
    """Raised when a local tool fails while handling a remote call."""


class ToolFatalError(ToolExecutionError):
    """Raised after a tool reports a fatal device condition."""


class ToolApprovalDenied(LabOSMCPError):
    """Raised when a human operator denies a requested tool approval."""


class ApprovalTimeoutError(LabOSMCPError):
    """Raised when an approval request does not receive a response in time."""
