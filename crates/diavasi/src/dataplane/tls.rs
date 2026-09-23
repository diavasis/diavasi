use std::path::{Path, PathBuf};

/// CA PEM, leaf certificate PEM, and leaf private key PEM.
pub fn generate_self_signed() -> Result<(String, String, String), rcgen::Error> {
    let mut ca_params = rcgen::CertificateParams::new(vec!["Diavasi Data Plane CA".to_string()])?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Diavasi Data Plane CA");
    let ca_key = rcgen::KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;

    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()])?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        )));
    let key_pair = rcgen::KeyPair::generate()?;
    let cert = params.signed_by(&key_pair, &ca_cert, &ca_key)?;
    Ok((ca_cert.pem(), cert.pem(), key_pair.serialize_pem()))
}

pub fn ca_path_for(cert_path: &Path) -> PathBuf {
    cert_path.with_file_name("dataplane-ca.crt")
}

/// Load PEM files when the leaf, key, and CA exist; otherwise generate and write them.
pub fn load_or_generate_pem(
    cert_path: &Path,
    key_path: &Path,
) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    let ca_path = ca_path_for(cert_path);
    if cert_path.exists() && key_path.exists() && ca_path.exists() {
        let cert = std::fs::read(cert_path)?;
        let key = std::fs::read(key_path)?;
        return Ok((cert, key));
    }
    if let Some(parent) = cert_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let (ca, cert, key) = generate_self_signed()?;
    std::fs::write(&ca_path, &ca)?;
    std::fs::write(cert_path, &cert)?;
    std::fs::write(key_path, &key)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(key_path, std::fs::Permissions::from_mode(0o600))?;
    }
    tracing::info!(
        ca = %ca_path.display(),
        cert = %cert_path.display(),
        key = %key_path.display(),
        "wrote data-plane TLS certificate"
    );
    Ok((cert.into_bytes(), key.into_bytes()))
}
