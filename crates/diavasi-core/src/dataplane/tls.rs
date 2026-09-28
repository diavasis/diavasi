use std::path::{Path, PathBuf};

/// A local CA and a leaf certificate it signs. The leaf covers `localhost`,
/// `127.0.0.1`, and each of `extra_sans` (a DNS name, or an IP address).
/// Returns the CA PEM, the leaf certificate PEM, and the leaf key PEM.
///
/// ```
/// use diavasi::dataplane::generate_self_signed;
/// let (ca, cert, key) = generate_self_signed(&["diavasi.internal".into(), "10.0.0.5".into()])?;
/// assert!(ca.starts_with("-----BEGIN CERTIFICATE-----"));
/// assert!(cert.starts_with("-----BEGIN CERTIFICATE-----"));
/// assert!(key.contains("PRIVATE KEY"));
/// # Ok::<(), rcgen::Error>(())
/// ```
pub fn generate_self_signed(
    extra_sans: &[String],
) -> Result<(String, String, String), rcgen::Error> {
    let _ = rustls::crypto::ring::default_provider().install_default();
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
    for san in extra_sans {
        let entry = match san.parse::<std::net::IpAddr>() {
            Ok(ip) => rcgen::SanType::IpAddress(ip),
            Err(_) => rcgen::SanType::DnsName(san.clone().try_into()?),
        };
        params.subject_alt_names.push(entry);
    }
    let key_pair = rcgen::KeyPair::generate()?;
    let cert = params.signed_by(&key_pair, &ca_cert, &ca_key)?;
    Ok((ca_cert.pem(), cert.pem(), key_pair.serialize_pem()))
}

/// Where the CA of a generated certificate is written: `dataplane-ca.crt`
/// next to the certificate.
pub fn ca_path_for(cert_path: &Path) -> PathBuf {
    cert_path.with_file_name("dataplane-ca.crt")
}

/// Read an operator-supplied certificate and key. Missing files are an error.
pub fn load_pem(cert_path: &Path, key_path: &Path) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let read = |path: &Path, what: &str| {
        std::fs::read(path).map_err(|err| {
            std::io::Error::new(err.kind(), format!("{what} {}: {err}", path.display()))
        })
    };
    Ok((read(cert_path, "certificate")?, read(key_path, "key")?))
}

/// Load the certificate and key when both exist. When neither exists,
/// generate a local CA and a leaf certificate for `localhost`, `127.0.0.1`,
/// and `extra_sans`, and write the CA to `dataplane-ca.crt` next to the
/// certificate. Existing files are never overwritten, and a lone certificate
/// or key is an error.
pub fn load_or_generate_pem(
    cert_path: &Path,
    key_path: &Path,
    extra_sans: &[String],
) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    match (cert_path.exists(), key_path.exists()) {
        (true, true) => return load_pem(cert_path, key_path),
        (false, false) => {}
        (true, false) | (false, true) => {
            return Err(std::io::Error::other(format!(
                "found only one of {} and {}; remove it or supply both",
                cert_path.display(),
                key_path.display()
            )));
        }
    }
    if let Some(parent) = cert_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let ca_path = ca_path_for(cert_path);
    let (ca, cert, key) = generate_self_signed(extra_sans)
        .map_err(|err| std::io::Error::other(format!("generating a certificate: {err}")))?;
    write_new(key_path, key.as_bytes(), 0o600)?;
    write_new(cert_path, cert.as_bytes(), 0o644)?;
    if !ca_path.exists() {
        write_new(&ca_path, ca.as_bytes(), 0o644)?;
    }
    tracing::info!(
        ca = %ca_path.display(),
        cert = %cert_path.display(),
        key = %key_path.display(),
        "wrote data-plane TLS certificate"
    );
    Ok((cert.into_bytes(), key.into_bytes()))
}

/// Create `path` with `contents`. Fails when the file already exists. On Unix
/// the file is created with `mode`, so a key is never readable by others,
/// even briefly.
fn write_new(path: &Path, contents: &[u8], mode: u32) -> std::io::Result<()> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
    }
    #[cfg(not(unix))]
    let _ = mode;
    options.open(path)?.write_all(contents)
}
