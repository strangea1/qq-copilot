use std::{path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::{MAX_IPC_MESSAGE_BYTES, config::AppConfig, ipc, protocol::BridgeRequest};

const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct InitializeParams {
    #[serde(rename = "protocolVersion")]
    protocol_version: String,
}

#[derive(Debug, Deserialize)]
struct CallToolParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SendProgressArgs {
    #[serde(default)]
    session_label: Option<String>,
    content: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SendFinalArgs {
    #[serde(default)]
    session_label: Option<String>,
    content: String,
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AskUserArgs {
    #[serde(default)]
    session_label: Option<String>,
    question: String,
    choices: Vec<String>,
    allow_freeform: bool,
    timeout_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitForMessageArgs {
    #[serde(default)]
    session_label: Option<String>,
    timeout_seconds: u64,
}

pub async fn run_stdio(config_path: &Path) -> Result<()> {
    let config = AppConfig::load(config_path)?;
    let session_id = std::env::var("QQ_COPILOT_SESSION_ID").ok();
    let mut reader = BufReader::new(io::stdin());
    let mut writer = io::stdout();
    let mut initialized = false;

    loop {
        let mut line = Vec::new();
        let read = reader
            .read_until(b'\n', &mut line)
            .await
            .context("failed to read MCP stdin")?;
        if read == 0 {
            return Ok(());
        }
        if line.len() > MAX_IPC_MESSAGE_BYTES {
            write_response(
                &mut writer,
                error_response(Value::Null, -32600, "MCP request exceeded the size limit"),
            )
            .await?;
            continue;
        }
        let request = match serde_json::from_slice::<JsonRpcRequest>(&line) {
            Ok(request) => request,
            Err(_) => {
                write_response(
                    &mut writer,
                    error_response(Value::Null, -32700, "Parse error"),
                )
                .await?;
                continue;
            }
        };
        if request.jsonrpc != "2.0" || request.method.trim().is_empty() {
            if let Some(id) = request.id {
                write_response(&mut writer, error_response(id, -32600, "Invalid Request")).await?;
            }
            continue;
        }

        let id = request.id.clone();
        let response = match request.method.as_str() {
            "initialize" => {
                let id = id.unwrap_or(Value::Null);
                match parse_params::<InitializeParams>(request.params) {
                    Ok(params) => {
                        initialized = true;
                        let selected = select_protocol_version(&params.protocol_version);
                        success_response(
                            id,
                            json!({
                                "protocolVersion": selected,
                                "capabilities": {
                                    "tools": {
                                        "listChanged": false
                                    }
                                },
                                "serverInfo": {
                                    "name": "qq-copilot-remote",
                                    "version": env!("CARGO_PKG_VERSION")
                                },
                                "instructions": "QQ tools always target the locally bound Owner. Do not send secrets, URLs, file paths, hidden reasoning, raw transcripts, or unredacted tool input."
                            }),
                        )
                    }
                    Err(error) => error_response(id, -32602, &error.to_string()),
                }
            }
            "notifications/initialized" | "notifications/cancelled" => continue,
            "ping" => {
                let Some(id) = id else {
                    continue;
                };
                success_response(id, json!({}))
            }
            "tools/list" => {
                let Some(id) = id else {
                    continue;
                };
                if !initialized {
                    error_response(id, -32002, "Server has not been initialized")
                } else {
                    success_response(id, json!({"tools": tool_definitions()}))
                }
            }
            "tools/call" => {
                let Some(id) = id else {
                    continue;
                };
                if !initialized {
                    error_response(id, -32002, "Server has not been initialized")
                } else {
                    match parse_params::<CallToolParams>(request.params) {
                        Ok(params) => match call_tool(&config, session_id.as_deref(), params).await
                        {
                            Ok(result) => success_response(id, tool_success(result)),
                            Err(error) => {
                                tracing::debug!(error = %error, "MCP tool call returned an error");
                                success_response(id, tool_error(&error.to_string()))
                            }
                        },
                        Err(error) => error_response(id, -32602, &error.to_string()),
                    }
                }
            }
            _ => {
                let Some(id) = id else {
                    continue;
                };
                error_response(id, -32601, "Method not found")
            }
        };
        write_response(&mut writer, response).await?;
    }
}

async fn call_tool(
    config: &AppConfig,
    session_id: Option<&str>,
    params: CallToolParams,
) -> Result<Value> {
    match params.name.as_str() {
        "qq_send_progress" => {
            let arguments: SendProgressArgs = serde_json::from_value(params.arguments)
                .context("invalid qq_send_progress arguments")?;
            ipc::call(
                config,
                BridgeRequest::SendProgress {
                    session_id: session_id.map(str::to_owned),
                    session_label: arguments.session_label,
                    content: arguments.content,
                },
                Duration::from_secs(30),
            )
            .await
        }
        "qq_send_final" => {
            let arguments: SendFinalArgs = serde_json::from_value(params.arguments)
                .context("invalid qq_send_final arguments")?;
            ipc::call(
                config,
                BridgeRequest::SendFinal {
                    session_id: session_id.map(str::to_owned),
                    session_label: arguments.session_label,
                    content: arguments.content,
                    idempotency_key: arguments.idempotency_key,
                },
                Duration::from_secs(30),
            )
            .await
        }
        "qq_ask_user" => {
            let arguments: AskUserArgs = serde_json::from_value(params.arguments)
                .context("invalid qq_ask_user arguments")?;
            let timeout_seconds = arguments
                .timeout_seconds
                .checked_add(15)
                .context("question timeout overflow")?;
            ipc::call(
                config,
                BridgeRequest::AskUser {
                    session_id: session_id.map(str::to_owned),
                    session_label: arguments.session_label,
                    question: arguments.question,
                    choices: arguments.choices,
                    allow_freeform: arguments.allow_freeform,
                    timeout_seconds: arguments.timeout_seconds,
                },
                Duration::from_secs(timeout_seconds),
            )
            .await
        }
        "qq_wait_for_message" => {
            let arguments: WaitForMessageArgs = serde_json::from_value(params.arguments)
                .context("invalid qq_wait_for_message arguments")?;
            let timeout_seconds = arguments
                .timeout_seconds
                .checked_add(15)
                .context("wait timeout overflow")?;
            ipc::call(
                config,
                BridgeRequest::WaitForMessage {
                    session_id: session_id.map(str::to_owned),
                    session_label: arguments.session_label,
                    timeout_seconds: arguments.timeout_seconds,
                },
                Duration::from_secs(timeout_seconds),
            )
            .await
        }
        _ => bail!("unknown QQ MCP tool {}", params.name),
    }
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "qq_send_progress",
            "description": "Send a short, redacted progress update to the single locally bound QQ Owner. This tool never accepts a recipient.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_label": {"type": "string", "minLength": 2, "maxLength": 16},
                    "content": {"type": "string", "minLength": 1, "maxLength": 6000}
                },
                "required": ["content"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "qq_send_final",
            "description": "Send exactly the same user-visible final answer to the bound QQ Owner once. Never include hidden reasoning, secrets, transcripts, URLs, or absolute paths.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_label": {"type": "string", "minLength": 2, "maxLength": 16},
                    "content": {"type": "string", "minLength": 1, "maxLength": 6000},
                    "idempotency_key": {"type": "string", "minLength": 1, "maxLength": 256}
                },
                "required": ["content", "idempotency_key"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "qq_ask_user",
            "description": "Ask the authenticated QQ Owner a question and block until an answer, cancellation, or timeout. Do not ask for secrets.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_label": {
                        "type": "string",
                        "minLength": 2,
                        "maxLength": 16,
                        "description": "Optional when exactly one Agent session is active"
                    },
                    "question": {"type": "string", "minLength": 1, "maxLength": 1000},
                    "choices": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1, "maxLength": 200},
                        "maxItems": 10
                    },
                    "allow_freeform": {"type": "boolean"},
                    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 600}
                },
                "required": ["question", "choices", "allow_freeform", "timeout_seconds"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "qq_wait_for_message",
            "description": "Wait for the next authenticated /message command explicitly addressed to the current Agent session. This tool must be auto-allowed.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_label": {"type": "string", "minLength": 2, "maxLength": 16},
                    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 300}
                },
                "required": ["timeout_seconds"],
                "additionalProperties": false
            }
        }),
    ]
}

fn parse_params<T: for<'de> Deserialize<'de>>(params: Option<Value>) -> Result<T> {
    serde_json::from_value(params.unwrap_or_else(|| json!({}))).context("invalid method parameters")
}

fn select_protocol_version(requested: &str) -> &str {
    SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .copied()
        .find(|version| *version == requested)
        .unwrap_or(DEFAULT_PROTOCOL_VERSION)
}

fn tool_success(result: Value) -> Value {
    json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string(&result).expect("JSON values serialize")
        }],
        "structuredContent": result,
        "isError": false
    })
}

fn tool_error(message: &str) -> Value {
    json!({
        "content": [{
            "type": "text",
            "text": message
        }],
        "isError": true
    })
}

fn success_response(id: Value, result: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: Some(result),
        error: None,
    }
}

fn error_response(id: Value, code: i64, message: &str) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.to_owned(),
        }),
    }
}

async fn write_response(writer: &mut io::Stdout, response: JsonRpcResponse) -> Result<()> {
    let mut serialized =
        serde_json::to_vec(&response).context("failed to serialize MCP response")?;
    serialized.push(b'\n');
    writer
        .write_all(&serialized)
        .await
        .context("failed to write MCP stdout")?;
    writer.flush().await.context("failed to flush MCP stdout")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_call_uses_protocol_name_field() {
        let request: JsonRpcRequest = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "qq_send_progress",
                "arguments": {
                    "session_label": "SABCD",
                    "content": "working"
                }
            }
        }))
        .expect("valid request");
        let params: CallToolParams = parse_params(request.params).expect("valid params");
        assert_eq!(params.name, "qq_send_progress");
    }

    #[test]
    fn session_label_is_optional_for_unique_active_session() {
        let arguments: SendFinalArgs = serde_json::from_value(json!({
            "content": "done",
            "idempotency_key": "session:turn:final"
        }))
        .expect("valid final arguments");
        assert!(arguments.session_label.is_none());
    }

    #[test]
    fn tool_call_accepts_standard_request_metadata() {
        let params: CallToolParams = serde_json::from_value(json!({
            "name": "qq_send_final",
            "arguments": {
                "content": "done",
                "idempotency_key": "session:turn:final"
            },
            "_meta": {
                "progressToken": "request-1"
            }
        }))
        .expect("tool call with MCP metadata");
        assert_eq!(params.name, "qq_send_final");
    }

    #[test]
    fn unsupported_protocol_falls_back_to_stable_version() {
        assert_eq!(
            select_protocol_version("2099-01-01"),
            DEFAULT_PROTOCOL_VERSION
        );
        assert_eq!(select_protocol_version("2024-11-05"), "2024-11-05");
    }

    #[test]
    fn initialize_accepts_standard_client_fields() {
        let params: InitializeParams = serde_json::from_value(json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {
                "roots": {"listChanged": true}
            },
            "clientInfo": {
                "name": "VS Code",
                "version": "1.0"
            }
        }))
        .expect("standard initialize params");
        assert_eq!(params.protocol_version, "2025-06-18");
    }

    #[test]
    fn all_tools_forbid_recipient_override() {
        for tool in tool_definitions() {
            assert!(
                tool["inputSchema"]["properties"]
                    .get("user_openid")
                    .is_none()
            );
            assert!(tool["inputSchema"]["properties"].get("recipient").is_none());
        }
    }
}
