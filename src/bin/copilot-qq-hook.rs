use std::{
    io::{self, Read, Write},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use qq_copilot_remote::{
    MAX_IPC_MESSAGE_BYTES,
    config::{AppConfig, default_config_path},
    ipc,
    protocol::{BridgeRequest, PermissionDecision, PermissionResult, StopDecision, StopResult},
};
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: HookCommand,
}

#[derive(Clone, Copy, Subcommand)]
enum HookCommand {
    Prompt,
    PreTool,
    PostTool,
    Stop,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(io::stderr)
        .without_time()
        .init();

    let cli = Cli::parse();
    let command = cli.command;
    let input = read_stdin_value();
    let stop_hook_active = input
        .as_ref()
        .ok()
        .and_then(|input| input.get("stop_hook_active"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let output = match async {
        let input = input?;
        run(cli, input).await
    }
    .await
    {
        Ok(output) => output,
        Err(error) => {
            tracing::error!(error = %error, "Hook failed");
            match command {
                HookCommand::PreTool => pre_tool_output(
                    PermissionDecision::Deny,
                    "QQ remote approval was unavailable; denied fail-closed",
                ),
                HookCommand::Stop if !stop_hook_active => stop_block_output(
                    "Before stopping, call qq_send_final once. QQ Bridge was unavailable, so the one-time local fallback blocked this stop.",
                ),
                HookCommand::Prompt | HookCommand::PostTool | HookCommand::Stop => json!({}),
            }
        }
    };

    match serde_json::to_string(&output) {
        Ok(output) => println!("{output}"),
        Err(error) => {
            eprintln!("failed to serialize Hook output: {error}");
            let fallback = b"{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"permissionDecision\":\"deny\",\"permissionDecisionReason\":\"Hook output serialization failed\"}}\n";
            if let Err(write_error) = io::stdout().write_all(fallback) {
                eprintln!("failed to write fallback Hook output: {write_error}");
            }
        }
    }
}

async fn run(cli: Cli, input: Value) -> Result<Value> {
    let config_path = cli.config.map_or_else(default_config_path, Ok)?;
    let config = AppConfig::load(&config_path)?;
    match cli.command {
        HookCommand::Prompt => {
            let session_id = required_string(&input, "session_id")?;
            let cwd = required_path(&input, "cwd")?;
            let hook_event_name = required_string(&input, "hook_event_name")?;
            let result = ipc::call(
                &config,
                BridgeRequest::RegisterPrompt { session_id, cwd },
                Duration::from_secs(10),
            )
            .await?;
            let session_label = result
                .get("session_label")
                .and_then(Value::as_str)
                .context("Bridge response omitted session_label")?;
            if hook_event_name == "SessionStart" {
                Ok(json!({
                    "hookSpecificOutput": {
                        "hookEventName": "SessionStart",
                        "additionalContext": format!(
                            "The authenticated remote QQ session label for this Agent session is {session_label}. Pass this exact label to QQ MCP tools when available."
                        )
                    }
                }))
            } else if hook_event_name == "UserPromptSubmit" {
                Ok(json!({}))
            } else {
                bail!("prompt hook received unsupported event {hook_event_name}");
            }
        }
        HookCommand::PreTool => {
            let session_id = required_string(&input, "session_id")?;
            let tool_use_id = required_string(&input, "tool_use_id")?;
            let tool_name = required_string(&input, "tool_name")?;
            let tool_input = input
                .get("tool_input")
                .cloned()
                .context("Hook input omitted tool_input")?;
            let cwd = required_path(&input, "cwd")?;
            let wait_seconds = config
                .bridge
                .bridge_wait_seconds
                .checked_add(15)
                .context("Bridge wait timeout overflow")?;
            let result = ipc::call(
                &config,
                BridgeRequest::PreTool {
                    session_id,
                    tool_use_id,
                    tool_name,
                    tool_input,
                    cwd,
                },
                Duration::from_secs(wait_seconds),
            )
            .await?;
            let result: PermissionResult =
                serde_json::from_value(result).context("invalid permission response")?;
            Ok(pre_tool_output(result.decision, &result.reason))
        }
        HookCommand::PostTool => {
            let session_id = required_string(&input, "session_id")?;
            let tool_use_id = required_string(&input, "tool_use_id")?;
            let tool_name = required_string(&input, "tool_name")?;
            ipc::call(
                &config,
                BridgeRequest::PostTool {
                    session_id,
                    tool_use_id,
                    tool_name,
                },
                Duration::from_secs(10),
            )
            .await?;
            Ok(json!({}))
        }
        HookCommand::Stop => {
            let session_id = required_string(&input, "session_id")?;
            let stop_hook_active = input
                .get("stop_hook_active")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let result = ipc::call(
                &config,
                BridgeRequest::Stop {
                    session_id,
                    stop_hook_active,
                },
                Duration::from_secs(20),
            )
            .await?;
            let result: StopResult =
                serde_json::from_value(result).context("invalid Stop response")?;
            Ok(match result.decision {
                StopDecision::Allow => json!({}),
                StopDecision::Block => stop_block_output(
                    result
                        .reason
                        .as_deref()
                        .unwrap_or("Send the final QQ response before stopping."),
                ),
            })
        }
    }
}

fn read_stdin_value() -> Result<Value> {
    let mut input = Vec::new();
    io::stdin()
        .take((MAX_IPC_MESSAGE_BYTES + 1) as u64)
        .read_to_end(&mut input)
        .context("failed to read Hook stdin")?;
    if input.is_empty() {
        bail!("Hook stdin was empty");
    }
    if input.len() > MAX_IPC_MESSAGE_BYTES {
        bail!("Hook stdin exceeded the size limit");
    }
    serde_json::from_slice(&input).context("Hook stdin was not valid JSON")
}

fn required_string(input: &Value, key: &str) -> Result<String> {
    input
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .with_context(|| format!("Hook input omitted {key}"))
}

fn required_path(input: &Value, key: &str) -> Result<PathBuf> {
    let path = PathBuf::from(required_string(input, key)?);
    if !path.is_absolute() {
        bail!("Hook {key} must be absolute");
    }
    Ok(path)
}

fn pre_tool_output(decision: PermissionDecision, reason: &str) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": match decision {
                PermissionDecision::Allow => "allow",
                PermissionDecision::Deny => "deny",
            },
            "permissionDecisionReason": reason,
        }
    })
}

fn stop_block_output(reason: &str) -> Value {
    json!({
        "hookSpecificOutput": {
            "hookEventName": "Stop",
            "decision": "block",
            "reason": reason,
        }
    })
}
