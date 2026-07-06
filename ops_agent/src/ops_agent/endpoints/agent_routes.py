# ops_agent/src/ops_agent/endpoints/agent_routes.py

import asyncio
import json
import logging
import os

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

# Import your native populated registries and schemas directly from your namespace
from ops_agent.tools.schemas import (
    AVAILABLE_TOOLS,
    LIST_PENDING_HOLDS_SCHEMA,
)

logger = logging.getLogger("OPS_AGENT.ROUTES")
agent_router = APIRouter()

LOCAL_LLM_URL = os.getenv("LOCAL_LLM_URL", "http://localhost:11434/api/chat")
MODEL_NAME = os.getenv("LOCAL_LLM_MODEL", "llama3.1:8b")


class NaturalLanguageCommandRequest(BaseModel):
    """Rigid validation schema layout ensuring the NLP string payload contract is met."""

    prompt: str = Field(
        ...,
        description="The raw natural language operational directive typed by the administrator.",
        example="Show me all the orders currently frozen under fraud review for Kentucky",
    )


def normalize_tool_arguments(arguments: dict) -> dict:
    """Model Gateway Adaptation Layer. Unpacks string-escaped JSON primitives cleanly."""
    if not isinstance(arguments, dict):
        return arguments

    normalized = {}
    for key, val in arguments.items():
        if isinstance(val, str) and (val.startswith("[") or val.startswith("{")):
            try:
                normalized[key] = json.loads(val)
                logger.info(
                    f"🔧 [GATEWAY NORMALIZER]: Morphed string-escaped key '{key}' into clean runtime object."
                )
            except Exception as parse_err:
                logger.warning(
                    f"⚠️ [GATEWAY NORMALIZER]: Failed to parse key '{key}': {str(parse_err)}"
                )
                normalized[key] = val
        else:
            normalized[key] = val
    return normalized


@agent_router.post("/api/agent/command")
async def process_operator_nlp_directive(
    request_payload: NaturalLanguageCommandRequest,
):
    """
    Standard Clean Multi-Step ReAct Agent Loop. Executes sequential function calling
    against distributed cluster shards asynchronously and synthesizes data back to the user [1.1].
    """
    user_prompt = request_payload.prompt
    logger.info(
        f"🔮 [AI AGENT INGESTION]: Processing operator directive -> [{user_prompt}]"
    )

    # 🟢 SOLUTION: Enforce strict parameter mapping guards to completely eliminate parameter drops! [1.1]
    messages = [
        {
            "role": "system",
            "content": (
                "You are an elite, systems-aware enterprise operations platform agent. "
                "When a user directive instructs you to perform a modification, resolution, or mutation "
                "(using words like 'approve', 'release', 'reject', 'override', or 'cancel'), you MUST "
                "explicitly populate the 'action_verdict' parameter inside your tool function call. "
                "Use 'APPROVE' if they command releasing/approving, or 'REJECT' if they command rejecting/terminating. "
                "Never omit the 'action_verdict' parameter if an explicit action directive is requested."
            ),
        },
        {"role": "user", "content": user_prompt},
    ]

    tools_whitelist = [LIST_PENDING_HOLDS_SCHEMA]

    MAX_REASONING_STEPS = 5
    step_counter = 0
    executed_tool_traces = []

    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            while step_counter < MAX_REASONING_STEPS:
                step_counter += 1
                logger.info(
                    f"🧠 [REASONING LOOP STEP {step_counter}]: Querying Llama engine..."
                )

                outgoing_payload = {
                    "model": MODEL_NAME,
                    "messages": messages,
                    "tools": tools_whitelist,
                    "stream": False,
                }

                llm_response = await client.post(LOCAL_LLM_URL, json=outgoing_payload)
                if llm_response.status_code != 200:
                    raise Exception(
                        f"Local LLM engine returned unhealthy status: {llm_response.status_code}"
                    )

                response_data = llm_response.json()

                print(
                    f"\n=================== 🤖 [RAW OLLAMA WIRE PAYLOAD] STEP {step_counter} 🤖 ==================="
                )
                print(json.dumps(response_data, indent=2))
                print(
                    "=========================================================================================\n"
                )

                model_message = response_data["message"]
                tool_calls = model_message.get("tool_calls", [])

                if not tool_calls:
                    logger.info(
                        "🏁 [CONVERGENCE ACHIEVED]: Model emitted final summary text pass."
                    )
                    return {
                        "status": "COMPLETED",
                        "reasoning_steps_executed": step_counter,
                        "tools_utilized_count": len(executed_tool_traces),
                        "executed_tool_traces": executed_tool_traces,
                        "agent_summary": model_message["content"],
                    }

                messages.append(model_message)

                for tool_call in tool_calls:
                    tool_call_id = tool_call.get("id")
                    function_name = tool_call["function"]["name"]
                    raw_arguments = tool_call["function"]["arguments"]

                    clean_arguments = normalize_tool_arguments(raw_arguments)
                    logger.info(
                        f"⚡ [LLM TOOL REQUEST]: Model requested [{function_name}] | Args: {clean_arguments}"
                    )

                    if (
                        function_name in AVAILABLE_TOOLS
                        and AVAILABLE_TOOLS[function_name] is not None
                    ):
                        execute_tool_callable = AVAILABLE_TOOLS[function_name]
                        try:
                            # Execute the async tool natively using non-blocking await [1.1]
                            tool_result_payload = await execute_tool_callable(
                                **clean_arguments
                            )
                        except Exception as exec_err:
                            logger.error(
                                f"Tool native execution failure: {str(exec_err)}"
                            )
                            tool_result_payload = {
                                "error": f"Internal tool processing crash: {str(exec_err)}"
                            }
                    else:
                        tool_result_payload = {
                            "error": f"Function tool mapping failure for name: '{function_name}'"
                        }

                    executed_tool_traces.append(
                        {
                            "step": step_counter,
                            "tool_name": function_name,
                            "arguments_passed": clean_arguments,
                            "records_returned_count": len(
                                tool_result_payload.get("batch_details", [])
                            )
                            if isinstance(tool_result_payload, dict)
                            else len(tool_result_payload)
                            if isinstance(tool_result_payload, list)
                            else 0,
                            "tool_raw_output": tool_result_payload,  # Enforced for test verification scans [1.1]
                        }
                    )

                    appended_context_block = {
                        "role": "tool",
                        "name": function_name,
                        "content": json.dumps(tool_result_payload),
                    }
                    if tool_call_id:
                        appended_context_block["tool_call_id"] = tool_call_id

                    messages.append(appended_context_block)

            raise Exception(
                "Maximum autonomous reasoning loop step constraints breached before convergence."
            )

    except Exception as loop_err:
        logger.error(
            f"❌ Core AI reasoning thread exception loop trace: {str(loop_err)}"
        )
        raise HTTPException(
            status_code=500,
            detail=f"Local LLM reasoning mesh boundary failure: {str(loop_err)}",
        )
