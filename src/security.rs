use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::LazyLock,
};

use regex::Regex;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::{config::PolicyConfig, protocol::RiskLevel};

const REDACTED: &str = "[REDACTED]";

static SECRET_KEY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(authorization|api[_-]?key|app[_-]?secret|client[_-]?secret|cookie|credential|passwd|password|private[_-]?key|refresh[_-]?token|secret|session[_-]?token|token)",
    )
    .expect("valid secret-key regex")
});
static SECRET_VALUE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?im)(-----BEGIN [A-Z ]*PRIVATE KEY-----|(?:bearer|qqbot)\s+[A-Za-z0-9._~+/=-]{12,}|gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,}|sk-[A-Za-z0-9_-]{20,}|(?:(?:api|client|app)[_-]?secret|api[_-]?key|password|passwd|refresh[_-]?token|session[_-]?token)\s*(?::|=|\bis\b)\s*["']?[^\s"',;]{4,}|--(?:api-key|password|secret|token)\s+\S+)"#,
    )
    .expect("valid secret-value regex")
});
static FORBIDDEN_COMMAND: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(-encodedcommand\b|\bcredential\s*manager\b|\bcmdkey\b|\bdiskpart\b|\bformat(?:\.com)?\b|\breg(?:\.exe)?\s+(?:add|delete|import|restore)\b|\bremove-item\b[^\r\n]*(?:-recurse|-force)[^\r\n]*(?:\\users\\|\\windows\\)|\brunas\b|\bsc(?:\.exe)?\s+(?:config|create|delete|start|stop)\b|\bschtasks\b|\bset-mppreference\b|\bstart-process\b[^\r\n]*-verb\s+runas\b|\bwevtutil\s+cl\b)",
    )
    .expect("valid forbidden-command regex")
});
static SENSITIVE_PATH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(^|[\\/])(\.env(?:\.[^\\/]*)?|\.git-credentials|\.npmrc|\.pypirc|\.ssh|credentials?|id_(?:rsa|dsa|ecdsa|ed25519)(?:\.pub)?|cookies?(?:\.sqlite)?|login data|local state|key\d*\.db|cert\d*\.db)(?:$|[\\/])",
    )
    .expect("valid sensitive-path regex")
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyAction {
    AutoAllow,
    RequireApproval,
    Deny,
}

#[derive(Debug, Clone)]
pub struct Classification {
    pub action: PolicyAction,
    pub risk: RiskLevel,
    pub input_hash: String,
    pub redacted_input: Value,
    pub summary: String,
    pub reason: String,
}

pub struct Classifier<'a> {
    policy: &'a PolicyConfig,
    workspace_roots: &'a [PathBuf],
    protected_paths: &'a [PathBuf],
}

impl<'a> Classifier<'a> {
    pub fn new(
        policy: &'a PolicyConfig,
        workspace_roots: &'a [PathBuf],
        protected_paths: &'a [PathBuf],
    ) -> Self {
        Self {
            policy,
            workspace_roots,
            protected_paths,
        }
    }

    pub fn classify(&self, tool_name: &str, tool_input: &Value, cwd: &Path) -> Classification {
        let normalized_tool = normalize_tool_name(tool_name);
        let redacted_input = redact_json(tool_input);
        let canonical = canonical_json(&redacted_input);
        let input_hash = sha256_hex(canonical.as_bytes());

        if !self.cwd_is_allowed(cwd) {
            return denied(
                input_hash,
                redacted_input,
                "Workspace is outside the configured roots",
            );
        }

        if contains_secret_key(tool_input) || contains_secret_value(&canonical) {
            return denied(
                input_hash,
                redacted_input,
                "Tool input contains credential-like data",
            );
        }

        let paths = extract_paths(&normalized_tool, tool_input, cwd);
        if paths.iter().any(|path| is_sensitive_path(path))
            || paths.iter().any(|path| self.is_protected(path))
        {
            return denied(
                input_hash,
                redacted_input,
                "Access to credentials or bridge security files is forbidden",
            );
        }
        if paths.iter().any(|path| !self.path_is_in_workspace(path)) {
            return denied(
                input_hash,
                redacted_input,
                "Tool path is outside the configured workspace roots",
            );
        }

        if is_shell_tool(&normalized_tool) {
            return self.classify_shell(tool_input, input_hash, redacted_input, cwd);
        }

        if self
            .policy
            .auto_allow_tools
            .iter()
            .any(|allowed| normalize_tool_name(allowed) == normalized_tool)
        {
            return Classification {
                action: PolicyAction::AutoAllow,
                risk: RiskLevel::Low,
                input_hash,
                summary: summarize(&normalized_tool, &redacted_input, cwd),
                redacted_input,
                reason: "Explicit read-only or internal QQ tool allowlist".to_owned(),
            };
        }

        if self
            .policy
            .approval_tools
            .iter()
            .any(|allowed| normalize_tool_name(allowed) == normalized_tool)
        {
            if is_edit_tool(&normalized_tool) && paths.is_empty() {
                return denied(
                    input_hash,
                    redacted_input,
                    "The edit target could not be determined safely",
                );
            }
            return Classification {
                action: PolicyAction::RequireApproval,
                risk: if normalized_tool.contains("delete")
                    || normalized_tool.contains("network")
                    || normalized_tool == "web-fetch"
                {
                    RiskLevel::High
                } else {
                    RiskLevel::Medium
                },
                input_hash,
                summary: summarize(&normalized_tool, &redacted_input, cwd),
                redacted_input,
                reason: "Explicit single-use approval allowlist".to_owned(),
            };
        }

        denied(
            input_hash,
            redacted_input,
            "Unknown tools are denied by default",
        )
    }

    fn classify_shell(
        &self,
        tool_input: &Value,
        input_hash: String,
        redacted_input: Value,
        cwd: &Path,
    ) -> Classification {
        let Some(command) = extract_command(tool_input) else {
            return denied(
                input_hash,
                redacted_input,
                "Shell command could not be determined",
            );
        };
        let normalized_command = normalize_command(&command);
        if FORBIDDEN_COMMAND.is_match(&normalized_command)
            || command_has_unsafe_composition(&command)
        {
            return denied(
                input_hash,
                redacted_input,
                "Command matches a permanently forbidden operation",
            );
        }
        let allowed = self
            .policy
            .approved_command_prefixes
            .iter()
            .map(|prefix| normalize_command(prefix))
            .any(|prefix| {
                normalized_command == prefix
                    || normalized_command
                        .strip_prefix(&prefix)
                        .is_some_and(|suffix| suffix.starts_with(' '))
            });
        if !allowed {
            return denied(
                input_hash,
                redacted_input,
                "Command is not in the explicit build/test allowlist",
            );
        }
        Classification {
            action: PolicyAction::RequireApproval,
            risk: RiskLevel::Medium,
            input_hash,
            summary: format!(
                "工具: powershell\n操作: {}\n目录: {}",
                truncate(&redact_text(&command), 500),
                display_workspace_path(cwd)
            ),
            redacted_input,
            reason: "Allowlisted command requires one-time QQ approval".to_owned(),
        }
    }

    fn cwd_is_allowed(&self, cwd: &Path) -> bool {
        self.workspace_roots
            .iter()
            .any(|root| path_is_within(cwd, root))
    }

    fn path_is_in_workspace(&self, path: &Path) -> bool {
        self.workspace_roots
            .iter()
            .any(|root| path_is_within(path, root))
    }

    fn is_protected(&self, path: &Path) -> bool {
        self.protected_paths
            .iter()
            .any(|protected| path_is_within(path, protected))
    }
}

pub fn normalize_tool_name(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace(['_', '.'], "-")
}

pub fn redact_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut redacted = Map::new();
            for (key, value) in object {
                if SECRET_KEY.is_match(key) {
                    redacted.insert(key.clone(), Value::String(REDACTED.to_owned()));
                } else {
                    redacted.insert(key.clone(), redact_json(value));
                }
            }
            Value::Object(redacted)
        }
        Value::Array(values) => Value::Array(values.iter().map(redact_json).collect()),
        Value::String(value) => Value::String(redact_text(value)),
        primitive => primitive.clone(),
    }
}

pub fn redact_text(value: &str) -> String {
    SECRET_VALUE.replace_all(value, REDACTED).into_owned()
}

pub fn contains_secret_value(value: &str) -> bool {
    SECRET_VALUE.is_match(value)
}

pub fn validate_agent_content(content: &str, max_chars: usize) -> Result<String, String> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Err("content must not be empty".to_owned());
    }
    if trimmed.chars().count() > max_chars {
        return Err(format!("content exceeds the {max_chars}-character limit"));
    }
    Ok(redact_text(trimmed))
}

pub fn canonical_json(value: &Value) -> String {
    fn canonicalize(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let sorted: BTreeMap<_, _> = object
                    .iter()
                    .map(|(key, value)| (key.clone(), canonicalize(value)))
                    .collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(values) => Value::Array(values.iter().map(canonicalize).collect()),
            primitive => primitive.clone(),
        }
    }

    serde_json::to_string(&canonicalize(value)).expect("JSON values are always serializable")
}

pub fn sha256_hex(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

pub fn constant_time_eq(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.as_bytes()
        .iter()
        .zip(right.as_bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

pub fn random_code(prefix: Option<char>, random_len: usize) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut bytes = vec![0_u8; random_len];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut bytes);
    let body: String = bytes
        .into_iter()
        .map(|byte| ALPHABET[usize::from(byte) % ALPHABET.len()] as char)
        .collect();
    prefix.map_or(body.clone(), |prefix| format!("{prefix}{body}"))
}

pub fn workspace_hash(path: &Path) -> String {
    sha256_hex(normalize_path(path).to_string_lossy().as_bytes())
}

fn denied(input_hash: String, redacted_input: Value, reason: &str) -> Classification {
    Classification {
        action: PolicyAction::Deny,
        risk: RiskLevel::Forbidden,
        input_hash,
        summary: reason.to_owned(),
        redacted_input,
        reason: reason.to_owned(),
    }
}

fn is_shell_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "powershell" | "run-in-terminal" | "run-terminal-command" | "shell"
    )
}

fn is_edit_tool(tool_name: &str) -> bool {
    tool_name.contains("apply-patch")
        || tool_name.contains("insert-")
        || tool_name.contains("replace-")
        || tool_name.contains("delete")
        || tool_name.contains("rename")
        || tool_name.contains("write")
}

fn extract_command(value: &Value) -> Option<String> {
    ["command", "script", "input"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_str).map(str::to_owned))
}

fn extract_paths(tool_name: &str, value: &Value, cwd: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    collect_structured_paths(value, cwd, &mut paths);
    if tool_name.contains("apply-patch") {
        collect_patch_paths(value, cwd, &mut paths);
    }
    paths.sort();
    paths.dedup();
    paths
}

fn collect_structured_paths(value: &Value, cwd: &Path, paths: &mut Vec<PathBuf>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let normalized_key = key.to_ascii_lowercase();
                if matches!(
                    normalized_key.as_str(),
                    "cwd"
                        | "directory"
                        | "file"
                        | "file_path"
                        | "path"
                        | "paths"
                        | "relative_path"
                        | "root"
                        | "workspace"
                ) {
                    collect_path_values(value, cwd, paths);
                } else {
                    collect_structured_paths(value, cwd, paths);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_structured_paths(value, cwd, paths);
            }
        }
        _ => {}
    }
}

fn collect_path_values(value: &Value, cwd: &Path, paths: &mut Vec<PathBuf>) {
    match value {
        Value::String(value) => {
            paths.push(resolve_path(cwd, &non_glob_prefix(value)));
        }
        Value::Array(values) => {
            for value in values {
                collect_path_values(value, cwd, paths);
            }
        }
        _ => {}
    }
}

fn collect_patch_paths(value: &Value, cwd: &Path, paths: &mut Vec<PathBuf>) {
    let patch = value.as_str().or_else(|| {
        value
            .get("patch")
            .or_else(|| value.get("input"))
            .and_then(Value::as_str)
    });
    let Some(patch) = patch else {
        return;
    };
    for line in patch.lines() {
        for prefix in [
            "*** Add File: ",
            "*** Update File: ",
            "*** Delete File: ",
            "*** Move to: ",
        ] {
            if let Some(path) = line.strip_prefix(prefix) {
                paths.push(resolve_path(cwd, path.trim()));
            }
        }
    }
}

fn resolve_path(cwd: &Path, value: &str) -> PathBuf {
    let path = Path::new(value);
    if path.is_absolute() {
        normalize_path(path)
    } else {
        normalize_path(&cwd.join(path))
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let raw = path.as_os_str().to_string_lossy();
    let path = if let Some(rest) = raw.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{rest}"))
    } else if let Some(rest) = raw.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        path.to_path_buf()
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

pub fn path_is_within(path: &Path, root: &Path) -> bool {
    let path = normalize_path(path);
    let root = normalize_path(root);
    let path_components: Vec<_> = path.components().collect();
    let root_components: Vec<_> = root.components().collect();
    root_components.len() <= path_components.len()
        && root_components
            .iter()
            .zip(path_components.iter())
            .all(|(root, path)| component_eq(root, path))
}

fn is_sensitive_path(path: &Path) -> bool {
    let mut normalized = path.to_string_lossy().replace('\\', "/");
    if path.file_name().is_some() {
        normalized.push('/');
    }
    SENSITIVE_PATH.is_match(&normalized)
}

fn normalize_command(command: &str) -> String {
    command
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn contains_secret_key(value: &Value) -> bool {
    match value {
        Value::Object(object) => object
            .iter()
            .any(|(key, value)| SECRET_KEY.is_match(key) || contains_secret_key(value)),
        Value::Array(values) => values.iter().any(contains_secret_key),
        _ => false,
    }
}

fn command_has_unsafe_composition(command: &str) -> bool {
    ["\r", "\n", ";", "|", "&", "`", "$(", ">", "<"]
        .into_iter()
        .any(|token| command.contains(token))
        || command.contains(r"..\")
        || command.contains("../")
        || command.contains(r":\")
        || command.contains(r"\\")
}

fn non_glob_prefix(value: &str) -> String {
    let wildcard = value
        .char_indices()
        .find_map(|(index, character)| matches!(character, '*' | '?').then_some(index));
    let prefix = wildcard.map_or(value, |index| &value[..index]);
    let prefix = prefix.trim_end_matches(['\\', '/']);
    if prefix.is_empty() {
        ".".to_owned()
    } else {
        prefix.to_owned()
    }
}

fn component_eq(left: &Component<'_>, right: &Component<'_>) -> bool {
    let left = left.as_os_str().to_string_lossy();
    let right = right.as_os_str().to_string_lossy();
    if cfg!(windows) {
        left.eq_ignore_ascii_case(&right)
    } else {
        left == right
    }
}

fn summarize(tool_name: &str, value: &Value, cwd: &Path) -> String {
    let input = truncate(&canonical_json(value), 1000);
    format!(
        "工具: {tool_name}\n参数: {input}\n目录: {}",
        display_workspace_path(cwd)
    )
}

fn display_workspace_path(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .map_or_else(|| "[workspace]".to_owned(), |name| format!("...\\{name}"))
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::json;

    use super::*;
    use crate::config::PolicyConfig;

    fn classifier(root: &Path) -> Classifier<'static> {
        let policy = Box::leak(Box::new(PolicyConfig::default()));
        let roots = Box::leak(Box::new(vec![root.to_path_buf()]));
        let protected = Box::leak(Box::new(vec![root.join(".bridge")]));
        Classifier::new(policy, roots, protected)
    }

    #[test]
    fn canonical_json_sorts_object_keys() {
        let left = json!({"z": 1, "a": {"d": 2, "b": 3}});
        let right = json!({"a": {"b": 3, "d": 2}, "z": 1});
        assert_eq!(canonical_json(&left), canonical_json(&right));
    }

    #[test]
    fn redacts_secret_keys_and_values() {
        let value = json!({
            "password": "plain",
            "nested": "Authorization: Bearer abcdefghijklmnopqrstuvwxyz"
        });
        let redacted = redact_json(&value);
        assert_eq!(redacted["password"], REDACTED);
        assert!(!canonical_json(&redacted).contains("abcdefghijklmnopqrstuvwxyz"));
        assert_eq!(redact_text("password = hunter2"), REDACTED);
    }

    #[test]
    fn read_inside_workspace_is_auto_allowed() {
        let root = PathBuf::from(r"C:\work\repo");
        let result = classifier(&root).classify(
            "view",
            &json!({"path": root.join("src").join("lib.rs")}),
            &root,
        );
        assert_eq!(result.action, PolicyAction::AutoAllow);
    }

    #[test]
    fn secret_file_is_permanently_denied() {
        let root = PathBuf::from(r"C:\work\repo");
        let result = classifier(&root).classify("view", &json!({"path": root.join(".env")}), &root);
        assert_eq!(result.action, PolicyAction::Deny);
        assert_eq!(result.risk, RiskLevel::Forbidden);
    }

    #[test]
    fn unknown_tool_is_denied() {
        let root = PathBuf::from(r"C:\work\repo");
        let result = classifier(&root).classify("mystery_tool", &json!({}), &root);
        assert_eq!(result.action, PolicyAction::Deny);
    }

    #[test]
    fn allowlisted_build_requires_approval() {
        let root = PathBuf::from(r"C:\work\repo");
        let result = classifier(&root).classify(
            "powershell",
            &json!({"command": "cargo test --all", "cwd": root}),
            &root,
        );
        assert_eq!(result.action, PolicyAction::RequireApproval);
    }

    #[test]
    fn vscode_qq_mcp_tool_id_is_auto_allowed() {
        let root = PathBuf::from(r"C:\work\repo");
        let result = classifier(&root).classify(
            "mcp_qq-copilot-re_qq_send_final",
            &json!({
                "content": "Done",
                "idempotency_key": "session:turn:final"
            }),
            &root,
        );
        assert_eq!(result.action, PolicyAction::AutoAllow);
    }

    #[test]
    fn elevated_command_is_denied() {
        let root = PathBuf::from(r"C:\work\repo");
        let result = classifier(&root).classify(
            "powershell",
            &json!({"command": "Start-Process cmd -Verb RunAs", "cwd": root}),
            &root,
        );
        assert_eq!(result.action, PolicyAction::Deny);
    }

    #[test]
    fn agent_content_allows_plain_text_urls_and_paths() {
        assert!(validate_agent_content("visit https://example.com", 100).is_ok());
        assert!(validate_agent_content(r"open C:\public.txt", 100).is_ok());
        assert!(validate_agent_content("tests passed", 100).is_ok());
    }

    #[test]
    fn shell_composition_cannot_bypass_command_allowlist() {
        let root = PathBuf::from(r"C:\work\repo");
        let result = classifier(&root).classify(
            "powershell",
            &json!({"command": "cargo test; Remove-Item file.txt", "cwd": root}),
            &root,
        );
        assert_eq!(result.action, PolicyAction::Deny);
    }

    #[test]
    fn glob_outside_workspace_is_denied() {
        let root = PathBuf::from(r"C:\work\repo");
        let result =
            classifier(&root).classify("rg", &json!({"paths": [r"C:\Users\Other\*"]}), &root);
        assert_eq!(result.action, PolicyAction::Deny);
    }

    #[cfg(windows)]
    #[test]
    fn verbatim_and_regular_windows_paths_share_the_same_boundary() {
        let root = PathBuf::from(r"\\?\C:\Users\Example\workspace");
        let child = PathBuf::from(r"C:\Users\Example\workspace\src\main.rs");
        assert!(path_is_within(&child, &root));
        assert!(!path_is_within(
            Path::new(r"C:\Users\Example\workspace-other"),
            &root
        ));
    }
}
