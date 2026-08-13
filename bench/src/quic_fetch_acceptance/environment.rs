//! Factual, one-shot environment gates for the QUIC Fetch acceptance driver.

use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::process::Command;

const PROCESS_INFO_JXA: &str = r"ObjC.import('Foundation');
const info = $.NSProcessInfo.processInfo;
const text = 'thermal_state=' + Number(info.thermalState) + '\nlow_power_mode=' + (info.lowPowerModeEnabled ? 1 : 0) + '\n';
const data = $(text).dataUsingEncoding($.NSUTF8StringEncoding);
$.NSFileHandle.fileHandleWithStandardOutput.writeData(data);";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OsFacts {
    pub product_version: String,
    pub kernel_identity: String,
    pub boot_record: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PowerFacts {
    pub ac_power_active: bool,
    pub low_power_mode: bool,
    /// The public `NSProcessInfo.thermalState` numeric value; zero is nominal.
    pub thermal_state: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoopbackFacts {
    pub address: String,
    pub port: u16,
    pub closed_before_return: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentExpectations {
    pub product_version: Option<String>,
    pub kernel_identity: Option<String>,
    pub boot_record: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentPass {
    pub schema: String,
    pub status: String,
    pub os: OsFacts,
    pub power: PowerFacts,
    pub loopback: LoopbackFacts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentBlockCode {
    OsProbeFailed,
    OsMismatch,
    PowerProbeFailed,
    AcPowerInactive,
    LowPowerModeEnabled,
    ThermalStateNotNominal,
    LoopbackProbeFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentBlocked {
    pub schema: String,
    pub status: String,
    pub stage: String,
    pub code: EnvironmentBlockCode,
    pub detail: String,
    pub product_invocations: u8,
}

impl EnvironmentBlocked {
    fn new(code: EnvironmentBlockCode, detail: impl Into<String>) -> Self {
        Self {
            schema: "distd.quic.fetch.environment-blocked.v1".to_owned(),
            status: "BLOCKED".to_owned(),
            stage: "environment_preflight".to_owned(),
            code,
            detail: detail.into(),
            product_invocations: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {detail}")]
pub struct EnvironmentProbeError {
    pub code: String,
    pub detail: String,
}

impl EnvironmentProbeError {
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

/// Injectable boundary. Implementations perform one observation per method and never poll.
pub trait EnvironmentProbe {
    fn observe_os(&self) -> Result<OsFacts, EnvironmentProbeError>;
    fn observe_power(&self) -> Result<PowerFacts, EnvironmentProbeError>;
    fn bind_close_ipv4_loopback(&self) -> Result<LoopbackFacts, EnvironmentProbeError>;
}

/// The real macOS/public-API environment probe.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemEnvironmentProbe;

impl EnvironmentProbe for SystemEnvironmentProbe {
    fn observe_os(&self) -> Result<OsFacts, EnvironmentProbeError> {
        Ok(OsFacts {
            product_version: command_text("/usr/bin/sw_vers", &["-productVersion"], "sw_vers")?,
            kernel_identity: command_text("/usr/bin/uname", &["-a"], "uname")?,
            boot_record: command_text("/usr/bin/who", &["-b"], "who_boot")?,
        })
    }

    fn observe_power(&self) -> Result<PowerFacts, EnvironmentProbeError> {
        let battery = command_text("/usr/bin/pmset", &["-g", "batt"], "pmset_batt")?;
        let ac_power_active = battery
            .lines()
            .any(|line| line.contains("Now drawing from 'AC Power'"));

        let process_info = command_text(
            "/usr/bin/osascript",
            &["-l", "JavaScript", "-e", PROCESS_INFO_JXA],
            "ns_process_info",
        )?;
        let thermal_state = parse_u8_assignment(&process_info, "thermal_state")?;
        let low_power_mode = match parse_u8_assignment(&process_info, "low_power_mode")? {
            0 => false,
            1 => true,
            value => {
                return Err(EnvironmentProbeError::new(
                    "invalid_low_power_mode",
                    format!("NSProcessInfo returned unsupported low-power value {value}"),
                ));
            }
        };

        Ok(PowerFacts {
            ac_power_active,
            low_power_mode,
            thermal_state,
        })
    }

    fn bind_close_ipv4_loopback(&self) -> Result<LoopbackFacts, EnvironmentProbeError> {
        let socket =
            UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).map_err(|error| {
                EnvironmentProbeError::new(
                    "ipv4_loopback_bind",
                    format!("IPv4 loopback UDP bind failed: {error}"),
                )
            })?;
        let local = socket.local_addr().map_err(|error| {
            EnvironmentProbeError::new(
                "ipv4_loopback_local_addr",
                format!("cannot inspect bound IPv4 loopback socket: {error}"),
            )
        })?;
        let SocketAddr::V4(local) = local else {
            return Err(EnvironmentProbeError::new(
                "ipv4_loopback_family",
                "IPv4 loopback bind unexpectedly returned a non-IPv4 address",
            ));
        };
        if !local.ip().is_loopback() {
            return Err(EnvironmentProbeError::new(
                "ipv4_loopback_address",
                "UDP capability probe did not bind a loopback address",
            ));
        }
        let facts = LoopbackFacts {
            address: local.ip().to_string(),
            port: local.port(),
            closed_before_return: true,
        };
        drop(socket);
        Ok(facts)
    }
}

/// Run each factual gate once. No wall-clock or performance timing is read.
pub fn run_environment_preflight<P: EnvironmentProbe>(
    probe: &P,
    expectations: &EnvironmentExpectations,
) -> Result<EnvironmentPass, EnvironmentBlocked> {
    let os = probe.observe_os().map_err(|error| {
        EnvironmentBlocked::new(EnvironmentBlockCode::OsProbeFailed, error.to_string())
    })?;
    check_os(&os, expectations)?;

    let power = probe.observe_power().map_err(|error| {
        EnvironmentBlocked::new(EnvironmentBlockCode::PowerProbeFailed, error.to_string())
    })?;
    check_power(&power)?;

    let loopback = probe.bind_close_ipv4_loopback().map_err(|error| {
        EnvironmentBlocked::new(EnvironmentBlockCode::LoopbackProbeFailed, error.to_string())
    })?;
    check_loopback(&loopback)?;

    Ok(EnvironmentPass {
        schema: "distd.quic.fetch.environment-pass.v1".to_owned(),
        status: "PASS".to_owned(),
        os,
        power,
        loopback,
    })
}

pub fn check_os(
    facts: &OsFacts,
    expectations: &EnvironmentExpectations,
) -> Result<(), EnvironmentBlocked> {
    check_optional_exact(
        "product_version",
        &facts.product_version,
        expectations.product_version.as_deref(),
    )?;
    check_optional_exact(
        "kernel_identity",
        &facts.kernel_identity,
        expectations.kernel_identity.as_deref(),
    )?;
    check_optional_exact(
        "boot_record",
        &facts.boot_record,
        expectations.boot_record.as_deref(),
    )
}

fn check_optional_exact(
    label: &str,
    actual: &str,
    expected: Option<&str>,
) -> Result<(), EnvironmentBlocked> {
    if let Some(expected) = expected {
        if actual != expected {
            return Err(EnvironmentBlocked::new(
                EnvironmentBlockCode::OsMismatch,
                format!("{label} changed from the frozen expectation"),
            ));
        }
    }
    Ok(())
}

pub fn check_power(facts: &PowerFacts) -> Result<(), EnvironmentBlocked> {
    if !facts.ac_power_active {
        return Err(EnvironmentBlocked::new(
            EnvironmentBlockCode::AcPowerInactive,
            "AC power is not active",
        ));
    }
    if facts.low_power_mode {
        return Err(EnvironmentBlocked::new(
            EnvironmentBlockCode::LowPowerModeEnabled,
            "Low Power Mode is enabled",
        ));
    }
    if facts.thermal_state != 0 {
        return Err(EnvironmentBlocked::new(
            EnvironmentBlockCode::ThermalStateNotNominal,
            format!(
                "OS thermal state is not nominal (state {})",
                facts.thermal_state
            ),
        ));
    }
    Ok(())
}

pub fn check_loopback(facts: &LoopbackFacts) -> Result<(), EnvironmentBlocked> {
    if facts.address != Ipv4Addr::LOCALHOST.to_string()
        || facts.port == 0
        || !facts.closed_before_return
    {
        return Err(EnvironmentBlocked::new(
            EnvironmentBlockCode::LoopbackProbeFailed,
            "IPv4 UDP loopback bind-close evidence is incomplete",
        ));
    }
    Ok(())
}

fn command_text(
    program: &str,
    arguments: &[&str],
    code: &str,
) -> Result<String, EnvironmentProbeError> {
    let output = Command::new(program)
        .args(arguments)
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .output()
        .map_err(|error| {
            EnvironmentProbeError::new(code, format!("failed to invoke {program}: {error}"))
        })?;
    if !output.status.success() {
        return Err(EnvironmentProbeError::new(
            code,
            format!(
                "{program} exited with status {}",
                output.status.code().unwrap_or(-1)
            ),
        ));
    }
    if !output.stderr.is_empty() {
        return Err(EnvironmentProbeError::new(
            code,
            format!("{program} produced unexpected stderr"),
        ));
    }
    let value = String::from_utf8(output.stdout)
        .map_err(|_| EnvironmentProbeError::new(code, format!("{program} output was not UTF-8")))?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(EnvironmentProbeError::new(
            code,
            format!("{program} produced empty output"),
        ));
    }
    Ok(value)
}

fn parse_u8_assignment(output: &str, key: &str) -> Result<u8, EnvironmentProbeError> {
    let matches: Vec<_> = output
        .lines()
        .filter_map(|line| line.split_once('='))
        .filter(|(candidate, _)| *candidate == key)
        .collect();
    if matches.len() != 1 {
        return Err(EnvironmentProbeError::new(
            "invalid_ns_process_info",
            format!("expected exactly one {key} assignment"),
        ));
    }
    matches[0].1.parse::<u8>().map_err(|_| {
        EnvironmentProbeError::new(
            "invalid_ns_process_info",
            format!("{key} was not an unsigned integer"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[derive(Clone)]
    struct MockProbe {
        os: Result<OsFacts, EnvironmentProbeError>,
        power: Result<PowerFacts, EnvironmentProbeError>,
        loopback: Result<LoopbackFacts, EnvironmentProbeError>,
        calls: RefCell<Vec<&'static str>>,
    }

    impl MockProbe {
        fn passing() -> Self {
            Self {
                os: Ok(OsFacts {
                    product_version: "15.6".to_owned(),
                    kernel_identity: "Darwin fixture".to_owned(),
                    boot_record: "system boot fixture".to_owned(),
                }),
                power: Ok(PowerFacts {
                    ac_power_active: true,
                    low_power_mode: false,
                    thermal_state: 0,
                }),
                loopback: Ok(LoopbackFacts {
                    address: "127.0.0.1".to_owned(),
                    port: 42_424,
                    closed_before_return: true,
                }),
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl EnvironmentProbe for MockProbe {
        fn observe_os(&self) -> Result<OsFacts, EnvironmentProbeError> {
            self.calls.borrow_mut().push("os");
            self.os.clone()
        }

        fn observe_power(&self) -> Result<PowerFacts, EnvironmentProbeError> {
            self.calls.borrow_mut().push("power");
            self.power.clone()
        }

        fn bind_close_ipv4_loopback(&self) -> Result<LoopbackFacts, EnvironmentProbeError> {
            self.calls.borrow_mut().push("loopback");
            self.loopback.clone()
        }
    }

    fn exact_expectations() -> EnvironmentExpectations {
        EnvironmentExpectations {
            product_version: Some("15.6".to_owned()),
            kernel_identity: Some("Darwin fixture".to_owned()),
            boot_record: Some("system boot fixture".to_owned()),
        }
    }

    #[test]
    fn passing_mock_observes_each_gate_exactly_once() {
        let probe = MockProbe::passing();
        let receipt = run_environment_preflight(&probe, &exact_expectations()).unwrap();
        assert_eq!(receipt.status, "PASS");
        assert_eq!(&*probe.calls.borrow(), &["os", "power", "loopback"]);
    }

    #[test]
    fn os_drift_is_typed_blocked_and_stops_before_other_probes() {
        let probe = MockProbe::passing();
        let mut expectations = exact_expectations();
        expectations.boot_record = Some("different boot".to_owned());
        let blocked = run_environment_preflight(&probe, &expectations).unwrap_err();
        assert_eq!(blocked.status, "BLOCKED");
        assert_eq!(blocked.code, EnvironmentBlockCode::OsMismatch);
        assert_eq!(&*probe.calls.borrow(), &["os"]);
    }

    #[test]
    fn low_power_thermal_and_ac_failures_have_distinct_codes() {
        let mut probe = MockProbe::passing();
        probe.power = Ok(PowerFacts {
            ac_power_active: false,
            low_power_mode: false,
            thermal_state: 0,
        });
        assert_eq!(
            run_environment_preflight(&probe, &EnvironmentExpectations::default())
                .unwrap_err()
                .code,
            EnvironmentBlockCode::AcPowerInactive
        );

        let mut probe = MockProbe::passing();
        probe.power = Ok(PowerFacts {
            ac_power_active: true,
            low_power_mode: true,
            thermal_state: 0,
        });
        assert_eq!(
            run_environment_preflight(&probe, &EnvironmentExpectations::default())
                .unwrap_err()
                .code,
            EnvironmentBlockCode::LowPowerModeEnabled
        );

        let mut probe = MockProbe::passing();
        probe.power = Ok(PowerFacts {
            ac_power_active: true,
            low_power_mode: false,
            thermal_state: 1,
        });
        assert_eq!(
            run_environment_preflight(&probe, &EnvironmentExpectations::default())
                .unwrap_err()
                .code,
            EnvironmentBlockCode::ThermalStateNotNominal
        );
    }

    #[test]
    fn loopback_failure_is_typed_blocked() {
        let mut probe = MockProbe::passing();
        probe.loopback = Err(EnvironmentProbeError::new("bind", "denied"));
        let blocked =
            run_environment_preflight(&probe, &EnvironmentExpectations::default()).unwrap_err();
        assert_eq!(blocked.code, EnvironmentBlockCode::LoopbackProbeFailed);
        assert_eq!(&*probe.calls.borrow(), &["os", "power", "loopback"]);
    }

    #[test]
    fn assignment_parser_requires_one_exact_numeric_key() {
        assert_eq!(
            parse_u8_assignment("thermal_state=0\nlow_power_mode=1", "thermal_state").unwrap(),
            0
        );
        assert!(parse_u8_assignment("thermal_state=0\nthermal_state=1", "thermal_state").is_err());
        assert!(parse_u8_assignment("thermal_state=nominal", "thermal_state").is_err());
    }

    #[test]
    fn acceptance_receipts_reject_unknown_fields() {
        let mut value = serde_json::to_value(EnvironmentPass {
            schema: "distd.quic.fetch.environment-pass.v1".to_owned(),
            status: "PASS".to_owned(),
            os: MockProbe::passing().os.unwrap(),
            power: MockProbe::passing().power.unwrap(),
            loopback: MockProbe::passing().loopback.unwrap(),
        })
        .unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unauthenticated".to_owned(), serde_json::json!(true));

        assert!(serde_json::from_value::<EnvironmentPass>(value).is_err());
    }
}
