use tracing::info;
use rand::{distributions::Alphanumeric, Rng};

pub fn print_startup_string(
    pkg_description: &str,
    pkg_version: &str,
    git_version: Option<&str>,
    target: &str,
    built_time: &str,
    rustc_version: &str,
) {
    let git_information = match git_version {
        None => "".to_string(),
        Some(git) => format!(" (Git information: {git})"),
    };
    info!("Starting {}", pkg_description);
    info!(
        "This is version {}{}, built for {} by {} at {}",
        pkg_version, git_information, target, rustc_version, built_time
    )
}

pub fn print_shutdown_string() {
    info!("Exiting");
}

/// Formats a full controller name, which includes the operator and the controller
/// `operator` should be a FQDN-like string
/// `controller` should be the lower-case version of the primary resource name
pub fn format_full_controller_name(operator: &str, controller: &str) -> String {
    format!("{}-{}", operator, controller)
}

pub fn repair_resource_name(resource_name: &String) -> String {
    // TODO: more ...
    resource_name.replace('.', "-")
}

// get random name with length specified
pub fn generate_random_string(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generate_random_string_test() {
        let random_string = generate_random_string(10);
        assert!(!random_string.is_empty());
        println!("{}", random_string);
    }
}