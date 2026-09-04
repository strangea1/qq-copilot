use std::{
    io,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use fs2::FileExt;
use qq_copilot_remote::{
    config::{AhpAuthorizedTarget, AppConfig, default_config_path},
    db::Database,
    ipc,
    protocol::BridgeRequest,
    qq::{QqClient, QqMessenger, run_gateway},
    security::path_is_within,
    service::BridgeService,
};
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Init {
        #[arg(long = "workspace", required = true, value_name = "PATH")]
        workspaces: Vec<PathBuf>,
    },
    Run,
    Status,
    AhpSessions,
    AhpBind {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        session: String,
    },
    AhpUnbind,
    ConfigureAhp {
        #[arg(long = "workspace", required = true)]
        workspaces: Vec<PathBuf>,
        #[arg(long)]
        node: PathBuf,
        #[arg(long)]
        adapter_script: PathBuf,
        #[arg(long)]
        code: PathBuf,
        #[arg(long)]
        code_launcher: PathBuf,
        #[arg(long)]
        ssh: Option<PathBuf>,
    },
    AddWorkspace {
        #[arg(long = "workspace", required = true)]
        workspaces: Vec<PathBuf>,
    },
    RegisterLocalTarget {
        #[arg(long = "workspace")]
        workspace: PathBuf,
        #[arg(long, default_value_t = true)]
        open_vscode: bool,
        #[arg(long, default_value_t = 300)]
        trust_timeout_seconds: u64,
    },
    RegisterRemoteTarget {
        #[arg(long)]
        ssh_alias: String,
        #[arg(long = "workspace")]
        workspace: String,
        #[arg(long, default_value_t = true)]
        open_vscode: bool,
        #[arg(long, default_value_t = 300)]
        trust_timeout_seconds: u64,
    },
    RemoveLocalTarget {
        #[arg(long = "workspace")]
        workspace: PathBuf,
    },
    RemoveRemoteTarget {
        #[arg(long)]
        ssh_alias: String,
        #[arg(long = "workspace")]
        workspace: String,
    },
    TrustRequests,
    ReportTrust {
        #[arg(long = "workspace-uri", required = true)]
        workspace_uris: Vec<String>,
        #[arg(long)]
        trusted: bool,
    },
    SetMode {
        mode: IntegrationMode,
    },
    NewBindCode,
    StoreSecret,
    EmergencyDisable,
    EmergencyEnable,
}

#[derive(Clone, Copy, ValueEnum)]
enum IntegrationMode {
    Ahp,
    Legacy,
}

#[derive(Deserialize)]
struct CodeAgentEndpointsDocument {
    endpoints: Vec<CodeAgentEndpoint>,
}

#[derive(Deserialize)]
struct CodeAgentEndpoint {
    #[serde(rename = "schemaVersion")]
    schema_version: u32,
    #[serde(rename = "protocolVersion")]
    protocol_version: String,
    #[serde(rename = "type")]
    kind: String,
}

struct ValidatedRemoteTarget {
    canonical_path: String,
    user: String,
    host: String,
    port: u16,
    host_key_fingerprints: Vec<String>,
}

const SUPPORTED_AHP_PROTOCOLS: &[&str] = &["1.0.0", "0.8.0", "0.7.0", "0.6.0", "0.5.2", "0.5.1"];

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(io::stderr)
        .init();

    let cli = Cli::parse();
    let config_path = cli.config.map_or_else(default_config_path, Ok)?;
    match cli.command.unwrap_or(Command::Run) {
        Command::Init { workspaces } => initialize(&config_path, workspaces),
        Command::Run => run(&config_path).await,
        Command::Status => {
            let result = admin_call(&config_path, BridgeRequest::Status).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        Command::AhpSessions => {
            let result = admin_call(&config_path, BridgeRequest::AhpSessions).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        Command::AhpBind { endpoint, session } => {
            let result = admin_call(
                &config_path,
                BridgeRequest::AhpBindSession {
                    endpoint_id: endpoint,
                    session_uri: session,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        Command::AhpUnbind => {
            let result = admin_call(&config_path, BridgeRequest::AhpUnbindSession).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        Command::ConfigureAhp {
            workspaces,
            node,
            adapter_script,
            code,
            code_launcher,
            ssh,
        } => configure_ahp(
            &config_path,
            workspaces,
            &node,
            &adapter_script,
            &code,
            &code_launcher,
            ssh.as_deref(),
        ),
        Command::AddWorkspace { workspaces } => add_workspaces(&config_path, workspaces),
        Command::RegisterLocalTarget {
            workspace,
            open_vscode,
            trust_timeout_seconds,
        } => register_local_target(&config_path, &workspace, open_vscode, trust_timeout_seconds),
        Command::RegisterRemoteTarget {
            ssh_alias,
            workspace,
            open_vscode,
            trust_timeout_seconds,
        } => register_remote_target(
            &config_path,
            &ssh_alias,
            &workspace,
            open_vscode,
            trust_timeout_seconds,
        ),
        Command::RemoveLocalTarget { workspace } => remove_local_target(&config_path, &workspace),
        Command::RemoveRemoteTarget {
            ssh_alias,
            workspace,
        } => remove_remote_target(&config_path, &ssh_alias, &workspace),
        Command::TrustRequests => print_trust_requests(&config_path),
        Command::ReportTrust {
            workspace_uris,
            trusted,
        } => report_workspace_trust(&config_path, &workspace_uris, trusted),
        Command::SetMode { mode } => set_mode(&config_path, mode),
        Command::NewBindCode => {
            let result = admin_call(&config_path, BridgeRequest::NewBindCode).await?;
            let code = result
                .get("binding_code")
                .and_then(serde_json::Value::as_str)
                .context("Bridge response omitted binding_code")?;
            println!("{code}");
            Ok(())
        }
        Command::StoreSecret => {
            let config = AppConfig::load(&config_path)?;
            config.validate_daemon()?;
            let secret =
                rpassword::prompt_password("QQ AppSecret: ").context("failed to read AppSecret")?;
            config.store_app_secret(&secret)?;
            println!("QQ AppSecret stored and verified in Windows Credential Manager.");
            Ok(())
        }
        Command::EmergencyDisable => {
            admin_call(
                &config_path,
                BridgeRequest::SetEmergencyEnabled { enabled: false },
            )
            .await?;
            println!("QQ remote control disabled.");
            Ok(())
        }
        Command::EmergencyEnable => {
            admin_call(
                &config_path,
                BridgeRequest::SetEmergencyEnabled { enabled: true },
            )
            .await?;
            println!("QQ remote control enabled.");
            Ok(())
        }
    }
}

fn initialize(config_path: &Path, workspaces: Vec<PathBuf>) -> Result<()> {
    let canonical_workspaces = canonicalize_workspaces(workspaces)?;
    AppConfig::write_new(config_path, canonical_workspaces)?;
    if let Err(error) = restrict_config_permissions(config_path) {
        let _ = std::fs::remove_file(config_path);
        return Err(error).context("generated config was removed because ACL hardening failed");
    }
    println!("Created {}", config_path.display());
    println!("Set qq.app_id, then run `qq-bridge store-secret` before starting.");
    Ok(())
}

async fn run(config_path: &Path) -> Result<()> {
    let config = Arc::new(AppConfig::load(config_path)?);
    config.validate_daemon()?;
    let lock_path = config.bridge.database_path.with_extension("lock");
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("failed to open daemon lock {}", lock_path.display()))?;
    lock_file
        .try_lock_exclusive()
        .context("another QQ Bridge daemon is already using this configuration")?;
    let database = Database::open(&config.bridge.database_path)?;
    database.recover_interrupted_state()?;
    database.purge_expired(config.bridge.audit_retention_days)?;

    let qq_client = Arc::new(QqClient::new(config.qq.clone(), config.app_secret()?)?);
    let messenger: Arc<dyn QqMessenger> = qq_client.clone();
    let service = Arc::new(BridgeService::new(
        config.clone(),
        config_path.to_path_buf(),
        database.clone(),
        messenger,
    ));

    if database.owner()?.is_none() {
        let code = database.create_binding_code(config.bridge.bind_code_ttl_seconds)?;
        eprintln!(
            "No QQ owner is bound. Send `/bind {code}` in QQ within {} seconds.",
            config.bridge.bind_code_ttl_seconds
        );
    }

    let gateway = {
        let qq_client = qq_client.clone();
        let database = database.clone();
        let service = service.clone();
        tokio::spawn(async move { run_gateway(qq_client, database, service).await })
    };
    let maintenance = {
        let database = database.clone();
        let retention_days = config.bridge.audit_retention_days;
        let ahp_retention_days = config.ahp.event_retention_days;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600));
            loop {
                interval.tick().await;
                if let Err(error) = database.purge_expired(retention_days) {
                    tracing::error!(error = %error, "database maintenance failed");
                }
                if let Err(error) = database.ahp_purge_events(ahp_retention_days) {
                    tracing::error!(error = %error, "AHP event maintenance failed");
                }
            }
        })
    };
    let adapter = if config.ahp.enabled && config.ahp.adapter_auto_start {
        let node = config
            .ahp
            .node_executable
            .clone()
            .context("AHP Node executable is not configured")?;
        let script = config
            .ahp
            .adapter_script
            .clone()
            .context("AHP Adapter script is not configured")?;
        let config_path = config_path.to_path_buf();
        Some(tokio::spawn(async move {
            if let Err(error) = supervise_ahp_adapter(node, script, config_path).await {
                tracing::error!(error = %error, "AHP Adapter supervisor stopped");
            }
        }))
    } else {
        None
    };

    tracing::info!(pipe = %config.pipe_path(), "QQ Bridge started");
    tokio::select! {
        result = ipc::serve(config, service) => result,
        result = gateway => {
            result.context("QQ Gateway task panicked")??;
            bail!("QQ Gateway task exited unexpectedly")
        }
        result = tokio::signal::ctrl_c() => {
            result.context("failed to listen for Ctrl+C")?;
            tracing::info!("QQ Bridge stopping");
            database.set_gateway_runtime("stopped", false)?;
            maintenance.abort();
            if let Some(adapter) = adapter {
                adapter.abort();
            }
            Ok(())
        }
    }
}

fn configure_ahp(
    config_path: &Path,
    workspaces: Vec<PathBuf>,
    node: &Path,
    adapter_script: &Path,
    code: &Path,
    code_launcher: &Path,
    ssh: Option<&Path>,
) -> Result<()> {
    let mut config = AppConfig::load(config_path)?;
    let workspaces = canonicalize_workspaces(workspaces)?;
    let node = process_path(node)
        .with_context(|| format!("failed to resolve Node executable {}", node.display()))?;
    let adapter_script = process_path(adapter_script).with_context(|| {
        format!(
            "failed to resolve AHP Adapter script {}",
            adapter_script.display()
        )
    })?;
    let code = process_path(code)
        .with_context(|| format!("failed to resolve VS Code executable {}", code.display()))?;
    let code_launcher = process_path(code_launcher).with_context(|| {
        format!(
            "failed to resolve VS Code launcher {}",
            code_launcher.display()
        )
    })?;
    let ssh = ssh
        .map(|path| {
            process_path(path)
                .with_context(|| format!("failed to resolve ssh executable {}", path.display()))
        })
        .transpose()?;
    for workspace in &workspaces {
        if !config
            .bridge
            .workspace_roots
            .iter()
            .any(|root| path_is_within(workspace, root))
        {
            config.bridge.workspace_roots.push(workspace.clone());
        }
    }
    config.ahp.enabled = true;
    config.ahp.shared_workspaces = workspaces;
    config.ahp.adapter_auto_start = true;
    config.ahp.node_executable = Some(node);
    config.ahp.adapter_script = Some(adapter_script);
    config.ahp.code_executable = Some(code);
    config.ahp.code_launcher = Some(code_launcher);
    if ssh.is_some() {
        config.ahp.ssh_executable = ssh;
    }
    config.qq.approval_buttons_enabled = true;
    config.qq.intents |= 1_u64 << 26;
    config.save(config_path)?;
    println!("AHP mode configured. Restart qq-bridge to start the Adapter.");
    print_target_workspaces(&config);
    Ok(())
}

fn add_workspaces(config_path: &Path, workspaces: Vec<PathBuf>) -> Result<()> {
    let mut config = AppConfig::load(config_path)?;
    let workspaces = canonicalize_workspaces(workspaces)?;
    let mut added = 0_usize;

    for workspace in workspaces {
        if !config
            .bridge
            .workspace_roots
            .iter()
            .any(|root| path_is_within(&workspace, root))
        {
            config.bridge.workspace_roots.push(workspace.clone());
        }
        if !config
            .ahp
            .shared_workspaces
            .iter()
            .any(|configured| paths_equal(configured, &workspace))
        {
            config.ahp.shared_workspaces.push(workspace);
            added += 1;
        }
    }

    config.save(config_path)?;
    println!("Added {added} target workspace(s).");
    print_target_workspaces(&config);
    println!("Restart qq-bridge and the AHP Adapter to apply the updated target list.");
    Ok(())
}

fn register_local_target(
    config_path: &Path,
    workspace: &Path,
    open_vscode: bool,
    trust_timeout_seconds: u64,
) -> Result<()> {
    let config = AppConfig::load(config_path)?;
    if !config.ahp.enabled {
        bail!("AHP mode is not configured");
    }
    let database_path = config.bridge.database_path.clone();
    let code = config
        .ahp
        .code_executable
        .clone()
        .context("ahp.code_executable is not configured")?;
    let code_launcher = config
        .ahp
        .code_launcher
        .clone()
        .context("ahp.code_launcher is not configured")?;
    ensure_local_ahp_support(&code)?;
    let workspace = process_path(workspace)
        .with_context(|| format!("failed to resolve workspace {}", workspace.display()))?;
    if !workspace.is_dir() {
        bail!("workspace is not a directory: {}", workspace.display());
    }
    let was_authorized = config
        .ahp
        .authorized_targets
        .iter()
        .any(|target| matches!(target, AhpAuthorizedTarget::Local { path } if paths_equal(path, &workspace)));
    let workspace_uri = url::Url::from_directory_path(&workspace)
        .map_err(|()| anyhow::anyhow!("failed to build workspace URI"))?
        .to_string();
    let database = Database::open(&config.bridge.database_path)?;
    let request_id = uuid::Uuid::new_v4().to_string();
    database.ahp_begin_trust_request(
        &request_id,
        &workspace_uri,
        open_vscode,
        trust_timeout_seconds,
    )?;
    if open_vscode {
        open_local_target_in_vscode(&code_launcher, &workspace)?;
    }
    wait_for_trust_request(&database, &request_id, trust_timeout_seconds)?;
    let mut config = AppConfig::load(config_path)?;
    if !config.ahp.enabled {
        bail!("AHP mode was disabled while workspace trust confirmation was pending");
    }
    if config.bridge.database_path != database_path {
        bail!(
            "Bridge database configuration changed while workspace trust confirmation was pending"
        );
    }
    let remains_authorized = config
        .ahp
        .authorized_targets
        .iter()
        .any(|target| matches!(target, AhpAuthorizedTarget::Local { path } if paths_equal(path, &workspace)));
    if was_authorized && !remains_authorized {
        bail!(
            "local target authorization was revoked while workspace trust confirmation was pending"
        );
    }
    if !config
        .bridge
        .workspace_roots
        .iter()
        .any(|root| path_is_within(&workspace, root))
    {
        config.bridge.workspace_roots.push(workspace.clone());
    }
    if !config
        .ahp
        .authorized_targets
        .iter()
        .any(|target| matches!(target, AhpAuthorizedTarget::Local { path } if paths_equal(path, &workspace)))
    {
        config
            .ahp
            .authorized_targets
            .push(AhpAuthorizedTarget::Local { path: workspace.clone() });
    }
    config.save(config_path)?;
    database.ahp_delete_trust_request(&request_id)?;
    println!(
        "Registered local target: {} (trusted workspace confirmed)",
        workspace.display()
    );
    Ok(())
}

fn register_remote_target(
    config_path: &Path,
    ssh_alias: &str,
    workspace: &str,
    open_vscode: bool,
    trust_timeout_seconds: u64,
) -> Result<()> {
    let config = AppConfig::load(config_path)?;
    if !config.ahp.enabled {
        bail!("AHP mode is not configured");
    }
    let database_path = config.bridge.database_path.clone();
    let code_launcher = config
        .ahp
        .code_launcher
        .clone()
        .context("ahp.code_launcher is not configured")?;
    let ssh = config
        .ahp
        .ssh_executable
        .clone()
        .context("ahp.ssh_executable is not configured")?;
    let alias_authorized_target = config
        .ahp
        .authorized_targets
        .iter()
        .find(|target| {
            matches!(
                target,
                AhpAuthorizedTarget::Ssh { alias, .. } if alias == ssh_alias
            )
        })
        .cloned();
    let expected_host_key_fingerprints =
        alias_authorized_target
            .as_ref()
            .and_then(|target| match target {
                AhpAuthorizedTarget::Ssh {
                    host_key_fingerprints,
                    ..
                } => Some(host_key_fingerprints.as_slice()),
                AhpAuthorizedTarget::Local { .. } => None,
            });
    let remote =
        validate_remote_target(&ssh, ssh_alias, workspace, expected_host_key_fingerprints)?;
    if let Some(AhpAuthorizedTarget::Ssh {
        user, host, port, ..
    }) = alias_authorized_target.as_ref()
        && (user != &remote.user || host != &remote.host || *port != remote.port)
    {
        bail!(
            "SSH Host alias identity changed; remove its existing authorized targets before registering it again"
        );
    }
    let target = AhpAuthorizedTarget::Ssh {
        alias: ssh_alias.to_owned(),
        path: remote.canonical_path.clone(),
        user: remote.user.clone(),
        host: remote.host.clone(),
        port: remote.port,
        host_key_fingerprints: remote.host_key_fingerprints.clone(),
    };
    let original_authorized_target = config
        .ahp
        .authorized_targets
        .iter()
        .find(|configured| configured.matches(&target))
        .cloned();
    let workspace_uri = remote_workspace_uri(ssh_alias, &remote.canonical_path)?;
    let database = Database::open(&config.bridge.database_path)?;
    let request_id = uuid::Uuid::new_v4().to_string();
    database.ahp_begin_trust_request(
        &request_id,
        &workspace_uri,
        open_vscode,
        trust_timeout_seconds,
    )?;
    if open_vscode {
        open_remote_target_in_vscode(&code_launcher, &workspace_uri)?;
    }
    wait_for_trust_request(&database, &request_id, trust_timeout_seconds)?;
    let mut config = AppConfig::load(config_path)?;
    if !config.ahp.enabled {
        bail!("AHP mode was disabled while workspace trust confirmation was pending");
    }
    if config.bridge.database_path != database_path {
        bail!(
            "Bridge database configuration changed while workspace trust confirmation was pending"
        );
    }
    if original_authorized_target.as_ref().is_some_and(|original| {
        !config
            .ahp
            .authorized_targets
            .iter()
            .any(|configured| configured.matches(original))
    }) {
        bail!("SSH target authorization changed while workspace trust confirmation was pending");
    }
    if !config
        .ahp
        .authorized_targets
        .iter()
        .any(|configured| configured.matches(&target))
    {
        config.ahp.authorized_targets.push(target);
    }
    config.save(config_path)?;
    database.ahp_delete_trust_request(&request_id)?;
    println!(
        "Registered remote target: ssh:{} {} (trusted workspace confirmed)",
        ssh_alias, remote.canonical_path
    );
    Ok(())
}

fn remove_local_target(config_path: &Path, workspace: &Path) -> Result<()> {
    let mut config = AppConfig::load(config_path)?;
    let workspace = process_removal_path(workspace)
        .with_context(|| format!("failed to normalize workspace {}", workspace.display()))?;
    let before = config.ahp.effective_authorized_targets().len();
    config.ahp.authorized_targets.retain(|target| {
        !matches!(target, AhpAuthorizedTarget::Local { path } if paths_equal(path, &workspace))
    });
    config
        .ahp
        .shared_workspaces
        .retain(|configured| !paths_equal(configured, &workspace));
    config.save(config_path)?;
    println!(
        "Removed {} local target(s) for {}.",
        before.saturating_sub(config.ahp.effective_authorized_targets().len()),
        workspace.display()
    );
    Ok(())
}

fn remove_remote_target(config_path: &Path, ssh_alias: &str, workspace: &str) -> Result<()> {
    let mut config = AppConfig::load(config_path)?;
    let workspace = normalize_remote_posix_path(workspace)?;
    let before = config.ahp.authorized_targets.len();
    config.ahp.authorized_targets.retain(|target| {
        !matches!(
            target,
            AhpAuthorizedTarget::Ssh { alias, path, .. }
                if alias == ssh_alias && path == &workspace
        )
    });
    config.save(config_path)?;
    println!(
        "Removed {} remote target(s) for ssh:{} {}.",
        before.saturating_sub(config.ahp.authorized_targets.len()),
        ssh_alias,
        workspace
    );
    Ok(())
}

fn print_trust_requests(config_path: &Path) -> Result<()> {
    let config = AppConfig::load(config_path)?;
    let database = Database::open(&config.bridge.database_path)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "requests": database.ahp_pending_trust_requests()?
        }))?
    );
    Ok(())
}

fn report_workspace_trust(
    config_path: &Path,
    workspace_uris: &[String],
    trusted: bool,
) -> Result<()> {
    let config = AppConfig::load(config_path)?;
    let database = Database::open(&config.bridge.database_path)?;
    let requested_uris = database
        .ahp_pending_trust_requests()?
        .into_iter()
        .filter(|request| {
            workspace_uris
                .iter()
                .any(|reported| workspace_uris_equal(&request.workspace_uri, reported))
        })
        .map(|request| request.workspace_uri)
        .collect::<Vec<_>>();
    let updated = database.ahp_report_workspace_trust(&requested_uris, trusted)?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({ "updated": updated }))?
    );
    Ok(())
}

fn wait_for_trust_request(
    database: &Database,
    request_id: &str,
    timeout_seconds: u64,
) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_seconds.max(1));
    loop {
        let request = database
            .ahp_trust_request(request_id)?
            .context("trust request expired before VS Code reported workspace trust")?;
        if request.trusted {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            database.ahp_delete_trust_request(request_id)?;
            bail!(
                "timed out waiting for the VS Code trust status extension to confirm this exact workspace"
            );
        }
        thread::sleep(Duration::from_millis(500));
    }
}

fn open_local_target_in_vscode(code_launcher: &Path, workspace: &Path) -> Result<()> {
    let status = std::process::Command::new(code_launcher)
        .arg("--new-window")
        .arg(workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("failed to launch VS Code for local workspace trust")?;
    if !status.success() {
        bail!("VS Code refused to open the local workspace for trust confirmation");
    }
    Ok(())
}

fn open_remote_target_in_vscode(code_launcher: &Path, workspace_uri: &str) -> Result<()> {
    let status = std::process::Command::new(code_launcher)
        .args(["--new-window", "--folder-uri", workspace_uri])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("failed to launch VS Code Remote SSH workspace URI")?;
    if !status.success() {
        bail!("failed to open the VS Code Remote SSH workspace URI");
    }
    Ok(())
}

fn ensure_local_ahp_support(code_executable: &Path) -> Result<()> {
    let host_status = std::process::Command::new(code_executable)
        .args(["agent", "host", "--help"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("failed to probe `code agent host`")?;
    if !host_status.success() {
        bail!("`code agent host` is not available in the configured VS Code build");
    }
    let start_status = std::process::Command::new(code_executable)
        .args(["agent", "host"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("failed to start or reuse the default local agent host")?;
    if !start_status.success() {
        bail!("the default local `code agent host` could not be started or reused");
    }
    let output = std::process::Command::new(code_executable)
        .args(["agent", "endpoints"])
        .output()
        .context("failed to query local agent endpoints")?;
    if !output.status.success() {
        bail!("`code agent endpoints` failed for the local host");
    }
    validate_endpoints_json(
        std::str::from_utf8(&output.stdout).context("local endpoints output was not UTF-8")?,
    )
}

fn validate_remote_target(
    ssh_executable: &Path,
    ssh_alias: &str,
    workspace: &str,
    expected_host_key_fingerprints: Option<&[String]>,
) -> Result<ValidatedRemoteTarget> {
    validate_ssh_alias(ssh_alias)?;
    let (user, host, port) = resolve_ssh_alias(ssh_executable, ssh_alias)?;
    let workspace = normalize_remote_posix_path(workspace)?;
    let identity_script = r#"set -eu
if [ "$(uname -s)" != "Linux" ]; then
  echo "__QQ_REMOTE_ERROR__ not_linux"
  exit 20
fi
canonical_path="$(realpath "$1")"
if [ ! -d "$canonical_path" ]; then
  echo "__QQ_REMOTE_ERROR__ not_directory"
  exit 22
fi
if ! command -v ssh-keygen >/dev/null 2>&1; then
  echo "__QQ_REMOTE_ERROR__ missing_ssh_keygen"
  exit 23
fi
host_key_fingerprints="$(
  for key in /etc/ssh/ssh_host_*_key.pub; do
    if [ -r "$key" ]; then
      ssh-keygen -E sha256 -lf "$key" | awk '{print $2}'
    fi
  done | LC_ALL=C sort -u
)"
if [ -z "$host_key_fingerprints" ]; then
  echo "__QQ_REMOTE_ERROR__ missing_host_keys"
  exit 24
fi
echo "__QQ_HOST_KEYS__"
printf '%s\n' "$host_key_fingerprints"
echo "__QQ_CANONICAL_PATH__"
printf '%s\n' "$canonical_path"
"#;
    let output = run_ssh_script(ssh_executable, ssh_alias, identity_script, &[&workspace])?;
    let host_keys_marker = "__QQ_HOST_KEYS__\n";
    let canonical_marker = "\n__QQ_CANONICAL_PATH__\n";
    let host_keys_start = output
        .find(host_keys_marker)
        .context("remote validation did not report host key fingerprints")?
        + host_keys_marker.len();
    let canonical_marker_start = output[host_keys_start..]
        .find(canonical_marker)
        .map(|offset| host_keys_start + offset)
        .context("remote validation did not report the canonical path")?;
    let host_key_fingerprints =
        parse_host_key_fingerprints(&output[host_keys_start..canonical_marker_start])?;
    let canonical_start = canonical_marker_start + canonical_marker.len();
    let canonical_path = output[canonical_start..].trim().to_owned();
    if let Some(expected) = expected_host_key_fingerprints {
        let mut expected = expected.to_vec();
        expected.sort();
        expected.dedup();
        if expected != host_key_fingerprints {
            bail!(
                "SSH host key fingerprints changed; remove the existing target locally before explicitly re-registering this alias"
            );
        }
    }
    let agent_script = r#"set -eu
if ! command -v code >/dev/null 2>&1; then
  echo "__QQ_REMOTE_ERROR__ missing_code"
  exit 21
fi
code agent host --help >/dev/null 2>&1
code agent host >/dev/null 2>&1
code agent endpoints
"#;
    let endpoints = run_ssh_script(ssh_executable, ssh_alias, agent_script, &[])?;
    validate_endpoints_json(endpoints.trim())?;
    Ok(ValidatedRemoteTarget {
        canonical_path: normalize_remote_posix_path(&canonical_path)?,
        user,
        host,
        port,
        host_key_fingerprints,
    })
}

fn resolve_ssh_alias(ssh_executable: &Path, ssh_alias: &str) -> Result<(String, String, u16)> {
    validate_ssh_alias(ssh_alias)?;
    let output = std::process::Command::new(ssh_executable)
        .args(["-G", ssh_alias])
        .output()
        .context("failed to resolve the SSH Host alias")?;
    if !output.status.success() {
        bail!("SSH Host alias `{ssh_alias}` could not be resolved");
    }
    let text = std::str::from_utf8(&output.stdout).context("ssh -G output was not UTF-8")?;
    let mut user = None;
    let mut host = None;
    let mut port = None;
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        match (parts.next(), parts.next()) {
            (Some("user"), Some(value)) => user = Some(value.to_owned()),
            (Some("hostname"), Some(value)) => host = Some(value.to_owned()),
            (Some("port"), Some(value)) => {
                port = Some(value.parse::<u16>().context("ssh alias port was invalid")?)
            }
            _ => {}
        }
    }
    Ok((
        user.context("ssh alias did not resolve a user")?,
        host.context("ssh alias did not resolve a hostname")?,
        port.context("ssh alias did not resolve a port")?,
    ))
}

fn run_ssh_script(
    ssh_executable: &Path,
    ssh_alias: &str,
    script: &str,
    args: &[&str],
) -> Result<String> {
    validate_ssh_alias(ssh_alias)?;
    let remote_command = std::iter::once("sh".to_owned())
        .chain(std::iter::once("-s".to_owned()))
        .chain(std::iter::once("--".to_owned()))
        .chain(args.iter().map(|arg| posix_shell_quote(arg)))
        .collect::<Vec<_>>()
        .join(" ");
    let mut command = std::process::Command::new(ssh_executable);
    command
        .arg("-T")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("StrictHostKeyChecking=yes")
        .arg("-o")
        .arg("ConnectTimeout=60")
        .arg(ssh_alias)
        .arg(remote_command);
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to start ssh.exe")?;
    let write_result = (|| -> Result<()> {
        use std::io::Write;
        let stdin = child
            .stdin
            .as_mut()
            .context("failed to open ssh stdin for remote validation")?;
        stdin
            .write_all(script.as_bytes())
            .context("failed to send the remote validation script")?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }
    drop(child.stdin.take());
    let output = child
        .wait_with_output()
        .context("failed to wait for remote validation")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "remote validation through ssh:{} failed: {}",
            ssh_alias,
            stderr.trim()
        );
    }
    String::from_utf8(output.stdout).context("remote validation output was not UTF-8")
}

fn validate_endpoints_json(raw: &str) -> Result<()> {
    let document: CodeAgentEndpointsDocument =
        serde_json::from_str(raw).context("agent endpoints output was not valid JSON")?;
    let compatible = document.endpoints.iter().any(|endpoint| {
        endpoint.schema_version == 2
            && endpoint.kind == "standalone"
            && SUPPORTED_AHP_PROTOCOLS
                .iter()
                .any(|supported| *supported == endpoint.protocol_version)
    });
    if !compatible {
        bail!(
            "no compatible standalone agent host endpoint was found; first connect/update with VS Code Remote SSH or update VS Code locally"
        );
    }
    Ok(())
}

fn normalize_remote_posix_path(path: &str) -> Result<String> {
    if !path.starts_with('/')
        || path
            .chars()
            .any(|character| character.is_control() || character == '\0')
    {
        bail!("remote target paths must be absolute POSIX paths");
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    Ok(format!("/{}", parts.join("/")))
}

fn validate_ssh_alias(alias: &str) -> Result<()> {
    if alias.is_empty()
        || alias.starts_with('-')
        || alias.len() > 255
        || alias.chars().any(|character| {
            !(character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-'))
        })
    {
        bail!("SSH Host alias contains unsupported characters");
    }
    Ok(())
}

fn posix_shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn parse_host_key_fingerprints(value: &str) -> Result<Vec<String>> {
    let mut fingerprints = value
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    fingerprints.sort();
    fingerprints.dedup();
    if fingerprints.is_empty()
        || fingerprints.iter().any(|fingerprint| {
            !fingerprint.starts_with("SHA256:") || fingerprint.chars().any(char::is_whitespace)
        })
    {
        bail!("remote host key fingerprints are invalid");
    }
    Ok(fingerprints)
}

fn remote_workspace_uri(ssh_alias: &str, workspace: &str) -> Result<String> {
    validate_ssh_alias(ssh_alias)?;
    let workspace = normalize_remote_posix_path(workspace)?;
    let mut url = url::Url::parse(&format!("vscode-remote://ssh-remote+{ssh_alias}/"))
        .context("failed to build VS Code Remote SSH workspace URI")?;
    url.set_path(&workspace);
    Ok(url.to_string())
}

fn workspace_uris_equal(expected: &str, reported: &str) -> bool {
    if expected == reported {
        return true;
    }
    let (Ok(expected), Ok(reported)) = (url::Url::parse(expected), url::Url::parse(reported))
    else {
        return false;
    };
    if expected.scheme() != reported.scheme() {
        return false;
    }
    match expected.scheme() {
        "file" => match (expected.to_file_path(), reported.to_file_path()) {
            (Ok(expected), Ok(reported)) => paths_equal(&expected, &reported),
            _ => false,
        },
        "vscode-remote" => {
            decoded_url_host(&expected) == decoded_url_host(&reported)
                && decoded_url_path(&expected) == decoded_url_path(&reported)
        }
        _ => false,
    }
}

fn decoded_url_host(url: &url::Url) -> Option<String> {
    url.host_str()
        .and_then(|host| urlencoding::decode(host).ok())
        .map(|host| host.into_owned())
}

fn decoded_url_path(url: &url::Url) -> Option<String> {
    let path = urlencoding::decode(url.path()).ok()?;
    normalize_remote_posix_path(&path).ok()
}

fn canonicalize_workspaces(workspaces: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    let mut canonical_workspaces: Vec<PathBuf> = Vec::with_capacity(workspaces.len());
    for workspace in workspaces {
        let workspace = process_path(&workspace)
            .with_context(|| format!("failed to resolve workspace {}", workspace.display()))?;
        if !workspace.is_dir() {
            bail!("workspace is not a directory: {}", workspace.display());
        }
        if !canonical_workspaces
            .iter()
            .any(|configured| paths_equal(configured, &workspace))
        {
            canonical_workspaces.push(workspace);
        }
    }
    Ok(canonical_workspaces)
}

fn paths_equal(left: &Path, right: &Path) -> bool {
    let (left, right) = match (process_path(left), process_path(right)) {
        (Ok(left), Ok(right)) => (left, right),
        _ => (left.to_path_buf(), right.to_path_buf()),
    };
    path_is_within(&left, &right) && path_is_within(&right, &left)
}

fn print_target_workspaces(config: &AppConfig) {
    println!("AHP authorized targets:");
    for target in config.ahp.effective_authorized_targets() {
        match target {
            AhpAuthorizedTarget::Local { path } => {
                println!("- local {}", path.display());
            }
            AhpAuthorizedTarget::Ssh { alias, path, .. } => {
                println!("- ssh:{alias} {path}");
            }
        }
    }
}

fn set_mode(config_path: &Path, mode: IntegrationMode) -> Result<()> {
    let mut config = AppConfig::load(config_path)?;
    config.ahp.enabled = matches!(mode, IntegrationMode::Ahp);
    config.save(config_path)?;
    println!(
        "Integration mode set to {}. Restart qq-bridge to apply.",
        match mode {
            IntegrationMode::Ahp => "AHP",
            IntegrationMode::Legacy => "legacy Hooks/MCP",
        }
    );
    Ok(())
}

async fn supervise_ahp_adapter(
    node: PathBuf,
    adapter_script: PathBuf,
    config_path: PathBuf,
) -> Result<()> {
    loop {
        let mut child = tokio::process::Command::new(&node)
            .arg(&adapter_script)
            .arg("--config")
            .arg(&config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .context("failed to start AHP Adapter")?;
        let status = child
            .wait()
            .await
            .context("failed to wait for AHP Adapter")?;
        tracing::warn!(%status, "AHP Adapter exited; restarting in five seconds");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

fn process_path(path: &Path) -> Result<PathBuf> {
    let canonical = std::fs::canonicalize(path)?;
    #[cfg(windows)]
    {
        let text = canonical.to_string_lossy();
        if let Some(path) = text.strip_prefix(r"\\?\UNC\") {
            return Ok(PathBuf::from(format!(r"\\{path}")));
        }
        if let Some(path) = text.strip_prefix(r"\\?\") {
            return Ok(PathBuf::from(path));
        }
    }
    Ok(canonical)
}

fn process_removal_path(path: &Path) -> Result<PathBuf> {
    if path
        .try_exists()
        .with_context(|| format!("failed to inspect path {}", path.display()))?
    {
        return process_path(path);
    }
    let absolute = std::path::absolute(path)?;
    #[cfg(windows)]
    {
        let text = absolute.to_string_lossy();
        if let Some(path) = text.strip_prefix(r"\\?\UNC\") {
            return Ok(PathBuf::from(format!(r"\\{path}")));
        }
        if let Some(path) = text.strip_prefix(r"\\?\") {
            return Ok(PathBuf::from(path));
        }
    }
    Ok(absolute)
}

async fn admin_call(config_path: &Path, request: BridgeRequest) -> Result<serde_json::Value> {
    let config = AppConfig::load(config_path)?;
    ipc::call(&config, request, Duration::from_secs(15)).await
}

#[cfg(windows)]
fn restrict_config_permissions(config_path: &Path) -> Result<()> {
    use std::process::Command;

    let user = match (std::env::var("USERDOMAIN"), std::env::var("USERNAME")) {
        (Ok(domain), Ok(username)) if !domain.is_empty() && !username.is_empty() => {
            format!("{domain}\\{username}")
        }
        (_, Ok(username)) if !username.is_empty() => username,
        _ => bail!("could not determine the current Windows user for ACL hardening"),
    };
    let parent = config_path
        .parent()
        .context("config path has no parent directory")?;
    let directory_status = Command::new("icacls.exe")
        .arg(parent)
        .arg("/inheritance:r")
        .arg("/grant:r")
        .arg(format!("{user}:(OI)(CI)(F)"))
        .status()
        .context("failed to run icacls for the config directory")?;
    if !directory_status.success() {
        bail!("icacls rejected the config directory ACL");
    }
    let file_status = Command::new("icacls.exe")
        .arg(config_path)
        .arg("/inheritance:r")
        .arg("/grant:r")
        .arg(format!("{user}:(F)"))
        .status()
        .context("failed to run icacls for the config file")?;
    if !file_status.success() {
        bail!("icacls rejected the config file ACL");
    }
    Ok(())
}

#[cfg(not(windows))]
fn restrict_config_permissions(_config_path: &Path) -> Result<()> {
    bail!("this project supports only Windows")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_workspaces_authorizes_targets_and_deduplicates_paths() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("config.toml");
        let first = directory.path().join("first");
        let second = directory.path().join("second");
        std::fs::create_dir(&first).expect("first workspace");
        std::fs::create_dir(&second).expect("second workspace");
        AppConfig::write_new(&config_path, vec![first.clone()]).expect("config");

        add_workspaces(
            &config_path,
            vec![first.clone(), second.clone(), second.clone()],
        )
        .expect("add workspaces");

        let config = AppConfig::load(&config_path).expect("load config");
        assert_eq!(config.ahp.shared_workspaces.len(), 2);
        assert!(paths_equal(&config.ahp.shared_workspaces[0], &first));
        assert!(paths_equal(&config.ahp.shared_workspaces[1], &second));
        assert!(
            config
                .bridge
                .workspace_roots
                .iter()
                .any(|root| paths_equal(root, &second))
        );
    }

    #[test]
    fn remove_local_target_revokes_legacy_and_structured_entries() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("config.toml");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config =
            AppConfig::write_new(&config_path, vec![workspace.clone()]).expect("config");
        config.ahp.shared_workspaces = vec![workspace.clone()];
        config
            .ahp
            .authorized_targets
            .push(AhpAuthorizedTarget::Local {
                path: workspace.clone(),
            });
        config.save(&config_path).expect("save config");
        std::fs::remove_dir(&workspace).expect("remove workspace before revoking target");

        remove_local_target(&config_path, &workspace).expect("remove target");

        let config = AppConfig::load(&config_path).expect("load config");
        assert!(config.ahp.shared_workspaces.is_empty());
        assert!(config.ahp.authorized_targets.is_empty());
    }

    #[test]
    fn remote_identity_helpers_reject_options_and_quote_shell_values() {
        assert!(validate_ssh_alias("dev-box_1.example").is_ok());
        assert!(validate_ssh_alias("-oProxyCommand=bad").is_err());
        assert!(validate_ssh_alias("dev;touch-bad").is_err());
        assert_eq!(
            posix_shell_quote("/srv/a'$(touch bad)"),
            "'/srv/a'\\''$(touch bad)'"
        );
        assert_eq!(
            parse_host_key_fingerprints("SHA256:b\nSHA256:a\nSHA256:b\n").expect("fingerprints"),
            vec!["SHA256:a", "SHA256:b"]
        );
    }

    #[test]
    fn trust_uri_matching_normalizes_remote_authority_and_path_encoding() {
        let expected =
            remote_workspace_uri("devbox", "/home/user/My Project").expect("workspace URI");
        assert!(workspace_uris_equal(
            &expected,
            "vscode-remote://ssh-remote%2Bdevbox/home/user/My%20Project"
        ));
        assert!(!workspace_uris_equal(
            &expected,
            "vscode-remote://ssh-remote%2Bother/home/user/My%20Project"
        ));
    }
}
