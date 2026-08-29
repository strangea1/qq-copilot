use std::{
    io,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use fs2::FileExt;
use qq_copilot_remote::{
    config::{AppConfig, default_config_path},
    db::Database,
    ipc,
    protocol::BridgeRequest,
    qq::{QqClient, QqMessenger, run_gateway},
    security::path_is_within,
    service::BridgeService,
};
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
    },
    AddWorkspace {
        #[arg(long = "workspace", required = true)]
        workspaces: Vec<PathBuf>,
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
        } => configure_ahp(&config_path, workspaces, &node, &adapter_script),
        Command::AddWorkspace { workspaces } => add_workspaces(&config_path, workspaces),
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
    path_is_within(left, right) && path_is_within(right, left)
}

fn print_target_workspaces(config: &AppConfig) {
    println!("AHP target workspaces:");
    for workspace in &config.ahp.shared_workspaces {
        println!("- {}", workspace.display());
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
        assert_eq!(config.ahp.shared_workspaces, vec![first, second.clone()]);
        assert!(
            config
                .bridge
                .workspace_roots
                .iter()
                .any(|root| paths_equal(root, &second))
        );
    }
}
