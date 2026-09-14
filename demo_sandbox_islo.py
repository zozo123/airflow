"""
Demo: SandboxToolset driving a real islo.dev microVM.

A deterministic pydantic-ai model calls each of the four sandbox tools in turn,
so the run is reproducible and needs no LLM. Reads ISLO_API_KEY from the
environment; nothing is printed that could leak it.
"""

from __future__ import annotations

import os
import sys
import time

from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from airflow.providers.common.ai.sandbox import IsloSandboxBackend
from airflow.providers.common.ai.toolsets import SandboxToolset

CYAN, GREEN, YELLOW, DIM, BOLD, RESET = (
    "\033[36m",
    "\033[32m",
    "\033[33m",
    "\033[2m",
    "\033[1m",
    "\033[0m",
)

STATE = "/tmp/agent_workspace/notes.txt"

# (tool, args, what this step demonstrates)
STEPS = [
    ("write_file", {"path": STATE, "content": "islo-sandbox-ok"}, "native streaming upload"),
    ("run_command", {"command": f"cat {STATE} && echo $((6 * 7))"}, "shell in the microVM"),
    ("read_file", {"path": STATE}, "state persists across tool calls"),
    ("run_command", {"command": "sleep 15 & echo returns-immediately"}, "a backgrounded process cannot stall the call"),
    ("run_command", {"command": "touch /tmp/f && ls -l /tmp/f | cut -c1-10"}, "agent keeps normal file permissions"),
    ("run_command", {"command": "curl -sS -m 5 https://example.com || echo '<egress denied>'"}, "deny-all network policy"),
    ("run_command", {"command": "echo boom >&2; exit 3"}, "failures come back as a retryable error"),
    ("list_directory", {"path": "/tmp/agent_workspace"}, "directory listing"),
]


def main() -> int:
    if not os.environ.get("ISLO_API_KEY"):
        print("set ISLO_API_KEY first", file=sys.stderr)
        return 1

    print(f"{BOLD}SandboxToolset -> islo.dev microVM{RESET}  {DIM}(deterministic model, no LLM){RESET}\n")
    step = iter(STEPS)
    pending: list[str] = []

    def model_function(messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
        returns = [
            part.content
            for message in messages
            for part in message.parts
            if part.part_kind == "tool-return"
        ]
        if returns:
            got = str(returns[-1]).strip().replace("\n", " ")
            print(f"  {GREEN}<-{RESET} {got[:96]}")
        try:
            tool, args, why = next(step)
        except StopIteration:
            return ModelResponse(parts=[TextPart(content="all sandbox tools exercised")])
        pending.append(tool)
        shown = args.get("command") or args.get("path", "")
        print(f"\n{CYAN}->{RESET} {BOLD}{tool}{RESET}  {DIM}# {why}{RESET}")
        print(f"  {YELLOW}{shown}{RESET}")
        time.sleep(0.35)
        return ModelResponse(
            parts=[ToolCallPart(tool_name=tool, args=args, tool_call_id=f"call-{len(pending)}")]
        )

    backend = IsloSandboxBackend(islo_conn_id=None, delete_after=900)
    agent = Agent(
        FunctionModel(model_function),
        instructions="Use the sandbox tools as requested.",
        toolsets=[SandboxToolset(backend)],
    )
    started = time.monotonic()
    result = agent.run_sync("Exercise the sandbox boundary.")
    print(f"\n{GREEN}{BOLD}{result.output}{RESET}  {DIM}in {time.monotonic() - started:.1f}s{RESET}")
    print(f"{DIM}sandbox destroyed; server-side TTL is the backstop{RESET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
