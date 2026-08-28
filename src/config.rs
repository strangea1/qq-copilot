use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};

const DEFAULT_CONFIG_FILE: &str = "config.toml";

#[derive(Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub bridge: BridgeConfig,
    pub qq: QqConfig,
    #[serde(default)]
    pub ahp: AhpConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BridgeConfig {
    pub pipe_name: String,
    pub ipc_token: String,
    pub database_path: PathBuf,
    #[serde(default)]
    pub workspace_roots: Vec<PathBuf>,
    #[serde(default = "default_approval_ttl_seconds")]
    pub approval_ttl_seconds: u64,
    #[serde(default = "default_bridge_wait_seconds")]
    pub bridge_wait_seconds: u64,
    #[serde(default = "default_bind_code_ttl_seconds")]
    pub bind_code_ttl_seconds: u64,
    #[serde(default = "default_question_ttl_seconds")]
    pub question_ttl_seconds: u64,
    #[serde(default = "default_chunk_chars")]
    pub message_chunk_chars: usize,
    #[serde(default = "default_total_chars")]
    pub message_total_chars: usize,
    #[serde(default = "default_per_minute_limit")]
    pub messages_per_minute: u32,
    #[serde(default = "default_daily_limit")]
    pub messages_per_day: u32,
    #[serde(default = "default_audit_retention_days")]
    pub audit_retention_days: u32,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QqConfig {
    pub app_id: String,
    #[serde(default)]
    pub app_secret_source: AppSecretSource,
    #[serde(default = "default_app_secret_env")]
    pub app_secret_env: String,
    #[serde(default = "default_credential_service")]
    pub credential_service: String,
    #[serde(default = "default_token_endpoint")]
    pub token_endpoint: String,
    #[serde(default = "default_api_base")]
    pub api_base: String,
    #[serde(default = "default_gateway_endpoint")]
    pub gateway_endpoint: String,
    #[serde(default = "default_intents")]
    pub intents: u64,
    #[serde(default = "default_token_refresh_skew_seconds")]
    pub token_refresh_skew_seconds: u64,
    #[serde(default)]
    pub approval_buttons_enabled: bool,
}

#[derive(Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppSecretSource {
    #[default]
    CredentialManager,
    Environment,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AhpConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub shared_workspace: Option<PathBuf>,
    #[serde(default = "default_ahp_event_retention_days")]
    pub event_retention_days: u32,
    #[serde(default = "default_ahp_command_lease_seconds")]
    pub command_lease_seconds: u64,
    #[serde(default = "default_ahp_poll_seconds")]
    pub poll_seconds: u64,
    #[serde(default = "default_ahp_stale_seconds")]
    pub adapter_stale_seconds: u64,
    #[serde(default)]
    pub adapter_auto_start: bool,
    #[serde(default)]
    pub node_executable: Option<PathBuf>,
    #[serde(default)]
    pub adapter_script: Option<PathBuf>,
    #[serde(default)]
    pub tool_notification_mode: AhpToolNotificationMode,
    #[serde(default = "default_typing_indicator_enabled")]
    pub typing_indicator_enabled: bool,
    #[serde(default = "default_typing_duration_seconds")]
    pub typing_duration_seconds: u8,
    #[serde(default = "default_typing_refresh_seconds")]
    pub typing_refresh_seconds: u8,
}

#[derive(Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AhpToolNotificationMode {
    Full,
    #[default]
    Compact,
}

impl Default for AhpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shared_workspace: None,
            event_retention_days: default_ahp_event_retention_days(),
            command_lease_seconds: default_ahp_command_lease_seconds(),
            poll_seconds: default_ahp_poll_seconds(),
            adapter_stale_seconds: default_ahp_stale_seconds(),
            adapter_auto_start: false,
            node_executable: None,
            adapter_script: None,
            tool_notification_mode: AhpToolNotificationMode::Compact,
            typing_indicator_enabled: default_typing_indicator_enabled(),
            typing_duration_seconds: default_typing_duration_seconds(),
            typing_refresh_seconds: default_typing_refresh_seconds(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    #[serde(default = "default_auto_allow_tools")]
    pub auto_allow_tools: Vec<String>,
    #[serde(default = "default_approval_tools")]
    pub approval_tools: Vec<String>,
    #[serde(default = "default_command_prefixes")]
    pub approved_command_prefixes: Vec<String>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            auto_allow_tools: default_auto_allow_tools(),
            approval_tools: default_approval_tools(),
            approved_command_prefixes: default_command_prefixes(),
        }
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let config: Self = toml::from_str(&content)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn write_new(path: &Path, workspace_roots: Vec<PathBuf>) -> Result<Self> {
        if path.exists() {
            bail!("refusing to overwrite existing config {}", path.display());
        }

        let parent = path
            .parent()
            .context("config path must have a parent directory")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;

        let mut token_bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut token_bytes);
        let mut pipe_suffix = [0_u8; 8];
        OsRng.fill_bytes(&mut pipe_suffix);

        let config = Self {
            bridge: BridgeConfig {
                pipe_name: format!("copilot-qq-bridge-{}", hex::encode(pipe_suffix)),
                ipc_token: hex::encode(token_bytes),
                database_path: parent.join("bridge.sqlite3"),
                workspace_roots,
                approval_ttl_seconds: default_approval_ttl_seconds(),
                bridge_wait_seconds: default_bridge_wait_seconds(),
                bind_code_ttl_seconds: default_bind_code_ttl_seconds(),
                question_ttl_seconds: default_question_ttl_seconds(),
                message_chunk_chars: default_chunk_chars(),
                message_total_chars: default_total_chars(),
                messages_per_minute: default_per_minute_limit(),
                messages_per_day: default_daily_limit(),
                audit_retention_days: default_audit_retention_days(),
            },
            qq: QqConfig {
                app_id: "CHANGE_ME".to_owned(),
                app_secret_source: AppSecretSource::CredentialManager,
                app_secret_env: default_app_secret_env(),
                credential_service: default_credential_service(),
                token_endpoint: default_token_endpoint(),
                api_base: default_api_base(),
                gateway_endpoint: default_gateway_endpoint(),
                intents: default_intents(),
                token_refresh_skew_seconds: default_token_refresh_skew_seconds(),
                approval_buttons_enabled: false,
            },
            ahp: AhpConfig::default(),
            policy: PolicyConfig::default(),
        };

        let serialized = toml::to_string_pretty(&config).context("failed to serialize config")?;
        fs::write(path, serialized)
            .with_context(|| format!("failed to write config {}", path.display()))?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.bridge.pipe_name.trim().is_empty()
            || self.bridge.pipe_name.contains('\\')
            || self.bridge.pipe_name.contains('/')
        {
            bail!("bridge.pipe_name must be a non-empty pipe name, not a path");
        }
        if self.bridge.ipc_token.len() < 64 {
            bail!("bridge.ipc_token must contain at least 256 bits");
        }
        if !self.bridge.database_path.is_absolute() {
            bail!("bridge.database_path must be absolute");
        }
        if self
            .bridge
            .workspace_roots
            .iter()
            .any(|path| !path.is_absolute())
        {
            bail!("all bridge.workspace_roots entries must be absolute");
        }
        if self.bridge.approval_ttl_seconds == 0
            || self.bridge.bridge_wait_seconds == 0
            || self.bridge.bridge_wait_seconds > self.bridge.approval_ttl_seconds
        {
            bail!("bridge wait must be non-zero and no longer than approval TTL");
        }
        if self.bridge.message_chunk_chars == 0
            || self.bridge.message_total_chars < self.bridge.message_chunk_chars
        {
            bail!("message length limits are invalid");
        }
        if self.qq.intents & (1_u64 << 25) == 0 {
            bail!("qq.intents must include GROUP_AND_C2C_EVENT (1 << 25)");
        }
        if self.qq.approval_buttons_enabled && self.qq.intents & (1_u64 << 26) == 0 {
            bail!("QQ approval buttons require INTERACTION intent (1 << 26)");
        }
        if self.ahp.enabled {
            let workspace = self
                .ahp
                .shared_workspace
                .as_ref()
                .context("ahp.shared_workspace is required when AHP is enabled")?;
            if !workspace.is_absolute() {
                bail!("ahp.shared_workspace must be absolute");
            }
            if self.ahp.event_retention_days == 0
                || self.ahp.command_lease_seconds == 0
                || self.ahp.poll_seconds == 0
                || self.ahp.poll_seconds > 60
                || self.ahp.adapter_stale_seconds == 0
            {
                bail!("AHP retention, lease, poll, and stale intervals must be non-zero");
            }
            if self.ahp.adapter_auto_start
                && (self.ahp.node_executable.is_none() || self.ahp.adapter_script.is_none())
            {
                bail!("ahp.node_executable and ahp.adapter_script are required for auto-start");
            }
            for path in [
                self.ahp.node_executable.as_ref(),
                self.ahp.adapter_script.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                if !path.is_absolute() {
                    bail!("AHP executable and script paths must be absolute");
                }
            }
            if self.ahp.typing_duration_seconds == 0
                || self.ahp.typing_duration_seconds > 60
                || self.ahp.typing_refresh_seconds == 0
                || self.ahp.typing_refresh_seconds >= self.ahp.typing_duration_seconds
            {
                bail!("AHP typing duration/refresh settings are invalid");
            }
        }
        Ok(())
    }

    pub fn validate_daemon(&self) -> Result<()> {
        if self.qq.app_id.trim().is_empty() || self.qq.app_id == "CHANGE_ME" {
            bail!("qq.app_id is not configured");
        }
        if self.qq.app_secret_source == AppSecretSource::Environment
            && self.qq.app_secret_env.trim().is_empty()
        {
            bail!("qq.app_secret_env must be configured for the environment source");
        }
        if self.qq.app_secret_source == AppSecretSource::CredentialManager
            && self.qq.credential_service.trim().is_empty()
        {
            bail!("qq.credential_service must not be empty");
        }
        if self.ahp.enabled && self.ahp.adapter_auto_start {
            let node = self
                .ahp
                .node_executable
                .as_ref()
                .context("AHP Node executable is not configured")?;
            let script = self
                .ahp
                .adapter_script
                .as_ref()
                .context("AHP Adapter script is not configured")?;
            if !node.is_file() {
                bail!("AHP Node executable does not exist");
            }
            if !script.is_file() {
                bail!("AHP Adapter script does not exist");
            }
            let script =
                fs::canonicalize(script).context("failed to resolve AHP Adapter script")?;
            for root in &self.bridge.workspace_roots {
                if let Ok(root) = fs::canonicalize(root)
                    && script.starts_with(root)
                {
                    bail!("AHP Adapter script must be installed outside Agent workspaces");
                }
            }
        }
        Ok(())
    }

    pub fn pipe_path(&self) -> String {
        format!(r"\\.\pipe\{}", self.bridge.pipe_name)
    }

    pub fn app_secret(&self) -> Result<String> {
        let secret = match self.qq.app_secret_source {
            AppSecretSource::CredentialManager => self
                .credential_entry()?
                .get_password()
                .context(
                    "QQ AppSecret was not found in Windows Credential Manager; run `qq-bridge store-secret` with the same --config path",
                )?,
            AppSecretSource::Environment => {
                env::var(&self.qq.app_secret_env).with_context(|| {
                    format!("environment variable {} is not set", self.qq.app_secret_env)
                })?
            }
        };
        if secret.trim().is_empty() {
            bail!("QQ AppSecret is empty");
        }
        Ok(secret)
    }

    pub fn store_app_secret(&self, secret: &str) -> Result<()> {
        if self.qq.app_secret_source != AppSecretSource::CredentialManager {
            bail!("qq.app_secret_source is not credential_manager");
        }
        if secret.trim().is_empty() {
            bail!("QQ AppSecret must not be empty");
        }
        let entry = self.credential_entry()?;
        entry
            .set_password(secret)
            .context("failed to store QQ AppSecret in Windows Credential Manager")?;
        let stored = entry
            .get_password()
            .context("QQ AppSecret could not be read back after storage")?;
        if stored != secret {
            bail!("QQ AppSecret verification failed after storage");
        }
        Ok(())
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let serialized = toml::to_string_pretty(self).context("failed to serialize config")?;
        fs::write(path, serialized)
            .with_context(|| format!("failed to update config {}", path.display()))
    }

    fn credential_entry(&self) -> Result<keyring::Entry> {
        use keyring::credential::CredentialPersistence;

        let persistence = keyring::default::default_credential_builder().persistence();
        if !matches!(persistence, CredentialPersistence::UntilDelete) {
            bail!(
                "the configured credential backend is not persistent; rebuild with the keyring windows-native feature"
            );
        }
        keyring::Entry::new(&self.qq.credential_service, &self.qq.app_id)
            .context("failed to open Windows Credential Manager entry")
    }
}

pub fn default_config_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("QQ_COPILOT_CONFIG") {
        return Ok(PathBuf::from(path));
    }
    let local_app_data = env::var_os("LOCALAPPDATA").context("LOCALAPPDATA is not set")?;
    Ok(PathBuf::from(local_app_data)
        .join("CopilotQQBridge")
        .join(DEFAULT_CONFIG_FILE))
}

fn default_approval_ttl_seconds() -> u64 {
    600
}

fn default_bridge_wait_seconds() -> u64 {
    600
}

fn default_bind_code_ttl_seconds() -> u64 {
    600
}

fn default_question_ttl_seconds() -> u64 {
    600
}

fn default_chunk_chars() -> usize {
    1800
}

fn default_total_chars() -> usize {
    6000
}

fn default_per_minute_limit() -> u32 {
    20
}

fn default_daily_limit() -> u32 {
    500
}

fn default_audit_retention_days() -> u32 {
    30
}

fn default_app_secret_env() -> String {
    "QQ_BOT_APP_SECRET".to_owned()
}

fn default_credential_service() -> String {
    "qq-copilot-remote".to_owned()
}

fn default_token_endpoint() -> String {
    "https://api.bot.qq.com/app/getAppAccessToken".to_owned()
}

fn default_api_base() -> String {
    "https://api.bot.qq.com".to_owned()
}

fn default_gateway_endpoint() -> String {
    "https://api.bot.qq.com/gateway".to_owned()
}

fn default_intents() -> u64 {
    1_u64 << 25
}

fn default_token_refresh_skew_seconds() -> u64 {
    60
}

fn default_ahp_event_retention_days() -> u32 {
    30
}

fn default_ahp_command_lease_seconds() -> u64 {
    60
}

fn default_ahp_poll_seconds() -> u64 {
    25
}

fn default_ahp_stale_seconds() -> u64 {
    45
}

fn default_typing_indicator_enabled() -> bool {
    true
}

fn default_typing_duration_seconds() -> u8 {
    60
}

fn default_typing_refresh_seconds() -> u8 {
    45
}

fn default_auto_allow_tools() -> Vec<String> {
    [
        "view",
        "rg",
        "glob",
        "oraios-serena-find-declaration",
        "oraios-serena-find-implementations",
        "oraios-serena-find-referencing-symbols",
        "oraios-serena-find-symbol",
        "oraios-serena-get-diagnostics-for-file",
        "oraios-serena-get-symbols-overview",
        "qq_send_progress",
        "qq_send_final",
        "qq_ask_user",
        "qq_wait_for_message",
        "mcp_qq-copilot-re_qq_send_progress",
        "mcp_qq-copilot-re_qq_send_final",
        "mcp_qq-copilot-re_qq_ask_user",
        "mcp_qq-copilot-re_qq_wait_for_message",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn default_approval_tools() -> Vec<String> {
    [
        "apply_patch",
        "powershell",
        "web_fetch",
        "web_search",
        "oraios-serena-insert-after-symbol",
        "oraios-serena-insert-before-symbol",
        "oraios-serena-rename-symbol",
        "oraios-serena-replace-content",
        "oraios-serena-replace-in-files",
        "oraios-serena-replace-symbol-body",
        "oraios-serena-safe-delete-symbol",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn default_command_prefixes() -> Vec<String> {
    [
        "cargo check",
        "cargo clippy",
        "cargo fmt",
        "cargo test",
        "dotnet build",
        "dotnet test",
        "go test",
        "npm run build",
        "npm run lint",
        "npm test",
        "pnpm build",
        "pnpm lint",
        "pnpm test",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[cfg(test)]
mod tests {
    #[cfg(windows)]
    use std::process::Command;

    #[cfg(windows)]
    use rand::{RngCore, rngs::OsRng};

    use super::*;

    #[test]
    fn generated_config_has_strong_local_token() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("config.toml");
        let root = directory.path().to_path_buf();

        let config = AppConfig::write_new(&config_path, vec![root.clone()]).expect("write config");

        assert_eq!(config.bridge.ipc_token.len(), 64);
        assert_eq!(config.bridge.workspace_roots, vec![root]);
        assert!(config.bridge.database_path.is_absolute());
        AppConfig::load(&config_path).expect("reload config");
    }

    #[cfg(windows)]
    #[test]
    fn credential_manager_persists_across_processes() {
        const CHILD_FLAG: &str = "QQ_COPILOT_CREDENTIAL_TEST_CHILD";
        const CONFIG_ENV: &str = "QQ_COPILOT_CREDENTIAL_TEST_CONFIG";
        const HASH_ENV: &str = "QQ_COPILOT_CREDENTIAL_TEST_HASH";

        if std::env::var_os(CHILD_FLAG).is_some() {
            let config_path = std::env::var(CONFIG_ENV).expect("credential test config");
            let expected_hash = std::env::var(HASH_ENV).expect("credential test hash");
            let config =
                AppConfig::load(Path::new(&config_path)).expect("load child credential config");
            let secret = config.app_secret().expect("cross-process credential read");
            assert_eq!(
                crate::security::sha256_hex(secret.as_bytes()),
                expected_hash
            );
            return;
        }

        let persistence = keyring::default::default_credential_builder().persistence();
        assert!(matches!(
            persistence,
            keyring::credential::CredentialPersistence::UntilDelete
        ));

        let directory = tempfile::tempdir().expect("tempdir");
        let config_path = directory.path().join("config.toml");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace).expect("workspace");
        let mut config =
            AppConfig::write_new(&config_path, vec![workspace]).expect("credential test config");
        config.qq.app_id = format!("credential-test-{}", uuid::Uuid::new_v4());
        std::fs::write(
            &config_path,
            toml::to_string_pretty(&config).expect("serialize credential test config"),
        )
        .expect("write credential test config");

        let mut secret_bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut secret_bytes);
        let secret = hex::encode(secret_bytes);
        let expected_hash = crate::security::sha256_hex(secret.as_bytes());
        config
            .store_app_secret(&secret)
            .expect("store cross-process credential");

        let child = Command::new(std::env::current_exe().expect("current test executable"))
            .arg("config::tests::credential_manager_persists_across_processes")
            .arg("--exact")
            .env(CHILD_FLAG, "1")
            .env(CONFIG_ENV, &config_path)
            .env(HASH_ENV, expected_hash)
            .status()
            .expect("run credential reader child process");
        let cleanup = config
            .credential_entry()
            .expect("parent credential entry")
            .delete_credential();
        assert!(child.success(), "credential reader child process failed");
        cleanup.expect("remove cross-process test credential");
    }
}
