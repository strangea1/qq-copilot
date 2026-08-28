use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    IPC_PROTOCOL_VERSION, MAX_IPC_MESSAGE_BYTES,
    config::AppConfig,
    protocol::{BridgeRequest, RpcEnvelope, RpcResponse},
    security::constant_time_eq,
    service::BridgeService,
};

#[cfg(windows)]
mod platform {
    use super::*;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions},
        time::{sleep, timeout},
    };

    pub async fn call(
        config: &AppConfig,
        request: BridgeRequest,
        request_timeout: Duration,
    ) -> Result<Value> {
        timeout(request_timeout, call_inner(config, request))
            .await
            .context("Bridge request timed out")?
    }

    async fn call_inner(config: &AppConfig, request: BridgeRequest) -> Result<Value> {
        let envelope = RpcEnvelope::new(config.bridge.ipc_token.clone(), request);
        let request_id = envelope.request_id;
        let serialized =
            serde_json::to_vec(&envelope).context("failed to serialize Bridge request")?;
        if serialized.len() > MAX_IPC_MESSAGE_BYTES {
            bail!("Bridge request exceeds the IPC size limit");
        }

        let pipe_path = config.pipe_path();
        let mut last_error = None;
        let mut client = None;
        for _ in 0..20 {
            match ClientOptions::new().open(&pipe_path) {
                Ok(opened) => {
                    client = Some(opened);
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
        let mut client = client.with_context(|| {
            format!(
                "failed to connect to QQ Bridge: {}",
                last_error
                    .map(|error| error.to_string())
                    .unwrap_or_else(|| "unknown error".to_owned())
            )
        })?;
        client
            .write_all(&serialized)
            .await
            .context("failed to write Bridge request")?;
        client
            .write_all(b"\n")
            .await
            .context("failed to terminate Bridge request")?;
        client
            .flush()
            .await
            .context("failed to flush Bridge request")?;

        let mut reader = BufReader::new(client);
        let mut response = Vec::new();
        reader
            .read_until(b'\n', &mut response)
            .await
            .context("failed to read Bridge response")?;
        if response.is_empty() {
            bail!("QQ Bridge closed without a response");
        }
        if response.len() > MAX_IPC_MESSAGE_BYTES {
            bail!("Bridge response exceeds the IPC size limit");
        }
        let response: RpcResponse =
            serde_json::from_slice(&response).context("Bridge response was invalid JSON")?;
        if response.version != IPC_PROTOCOL_VERSION || response.request_id != request_id {
            bail!("Bridge response identity or protocol version did not match");
        }
        if let Some(error) = response.error {
            bail!("{}: {}", error.code, error.message);
        }
        response.result.context("Bridge response omitted result")
    }

    pub async fn serve(config: Arc<AppConfig>, service: Arc<BridgeService>) -> Result<()> {
        let pipe_path = config.pipe_path();
        let mut first_instance = true;
        loop {
            let server = ServerOptions::new()
                .first_pipe_instance(first_instance)
                .reject_remote_clients(true)
                .create(&pipe_path)
                .with_context(|| format!("failed to create named pipe {pipe_path}"))?;
            first_instance = false;
            server
                .connect()
                .await
                .context("failed to accept named pipe client")?;
            let config = config.clone();
            let service = service.clone();
            tokio::spawn(async move {
                if let Err(error) = handle_connection(server, config, service).await {
                    tracing::error!(error = %error, "named pipe request failed");
                }
            });
        }
    }

    async fn handle_connection(
        server: NamedPipeServer,
        config: Arc<AppConfig>,
        service: Arc<BridgeService>,
    ) -> Result<()> {
        let mut reader = BufReader::new(server);
        let mut request = Vec::new();
        reader
            .read_until(b'\n', &mut request)
            .await
            .context("failed to read named pipe request")?;
        let mut server = reader.into_inner();
        let response = if request.is_empty() {
            RpcResponse::error(Uuid::nil(), "empty_request", "request was empty")
        } else if request.len() > MAX_IPC_MESSAGE_BYTES {
            RpcResponse::error(
                Uuid::nil(),
                "request_too_large",
                "request exceeded the IPC size limit",
            )
        } else {
            match serde_json::from_slice::<RpcEnvelope>(&request) {
                Ok(envelope) => process_envelope(&config, &service, envelope).await,
                Err(_) => RpcResponse::error(
                    Uuid::nil(),
                    "invalid_json",
                    "request was not valid Bridge JSON",
                ),
            }
        };
        let mut serialized =
            serde_json::to_vec(&response).context("failed to serialize Bridge response")?;
        serialized.push(b'\n');
        server
            .write_all(&serialized)
            .await
            .context("failed to write named pipe response")?;
        server
            .flush()
            .await
            .context("failed to flush named pipe response")?;
        Ok(())
    }

    async fn process_envelope(
        config: &AppConfig,
        service: &BridgeService,
        envelope: RpcEnvelope,
    ) -> RpcResponse {
        if envelope.version != IPC_PROTOCOL_VERSION {
            return RpcResponse::error(
                envelope.request_id,
                "unsupported_protocol",
                "IPC protocol version is not supported",
            );
        }
        if !constant_time_eq(&envelope.auth_token, &config.bridge.ipc_token) {
            return RpcResponse::error(
                envelope.request_id,
                "unauthorized",
                "IPC authentication failed",
            );
        }
        match service.dispatch(envelope.request).await {
            Ok(result) => RpcResponse::ok(envelope.request_id, result),
            Err(error) => {
                tracing::warn!(request_id = %envelope.request_id, error = %error, "Bridge request rejected");
                RpcResponse::error(envelope.request_id, "bridge_error", error.to_string())
            }
        }
    }
}

#[cfg(not(windows))]
mod platform {
    use super::*;

    pub async fn call(
        _config: &AppConfig,
        _request: BridgeRequest,
        _request_timeout: Duration,
    ) -> Result<Value> {
        bail!("QQ Copilot Bridge named pipes are supported only on Windows")
    }

    pub async fn serve(_config: Arc<AppConfig>, _service: Arc<BridgeService>) -> Result<()> {
        bail!("QQ Copilot Bridge named pipes are supported only on Windows")
    }
}

pub use platform::{call, serve};

#[cfg(all(test, windows))]
mod tests {
    use std::{fs, sync::Arc};

    use serde_json::json;

    use super::*;
    use crate::{
        config::AppConfig,
        db::Database,
        qq::{MockQqMessenger, QqMessenger},
        service::BridgeService,
    };

    #[tokio::test]
    async fn named_pipe_round_trip_requires_local_token() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        let config_directory = directory.path().join("config");
        fs::create_dir_all(&workspace).expect("workspace");
        fs::create_dir_all(&config_directory).expect("config directory");
        let workspace = fs::canonicalize(workspace).expect("canonical workspace");
        let config_path = config_directory.join("config.toml");
        let config = AppConfig::write_new(&config_path, vec![workspace]).expect("config");
        let database = Database::open(&config.bridge.database_path).expect("database");
        let messenger: Arc<dyn QqMessenger> = Arc::new(MockQqMessenger::new());
        let config = Arc::new(config);
        let service = Arc::new(BridgeService::new(
            config.clone(),
            config_path,
            database,
            messenger,
        ));
        let server = tokio::spawn(serve(config.clone(), service));

        let result = call(&config, BridgeRequest::Health, Duration::from_secs(5))
            .await
            .expect("health call");
        assert_eq!(result["status"], json!("ok"));

        let mut invalid_config = (*config).clone();
        invalid_config.bridge.ipc_token = "0".repeat(64);
        let error = call(
            &invalid_config,
            BridgeRequest::Health,
            Duration::from_secs(5),
        )
        .await
        .expect_err("invalid token must fail");
        assert!(error.to_string().contains("unauthorized"));
        server.abort();
    }
}
