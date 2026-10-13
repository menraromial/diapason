//! TLS client configuration (rustls, ring provider).

use diapason_core::config::{ConfigError, TlsConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

fn err(m: String) -> ConfigError {
    ConfigError(format!("tls: {m}"))
}

pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, ConfigError> {
    let f = std::fs::File::open(path).map_err(|e| err(format!("{}: {e}", path.display())))?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(f))
        .collect::<Result<_, _>>()
        .map_err(|e| err(format!("{}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(err(format!("{}: no certificate", path.display())));
    }
    Ok(certs)
}

pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, ConfigError> {
    let f = std::fs::File::open(path).map_err(|e| err(format!("{}: {e}", path.display())))?;
    rustls_pemfile::private_key(&mut BufReader::new(f))
        .map_err(|e| err(format!("{}: {e}", path.display())))?
        .ok_or_else(|| err(format!("{}: no private key", path.display())))
}

/// Accepts any certificate. Signatures are still checked, so the handshake
/// stays well formed.
#[derive(Debug)]
struct AcceptAny(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for AcceptAny {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

pub fn client_config(cfg: &TlsConfig, alpn: &[u8]) -> Result<Arc<ClientConfig>, ConfigError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| err(e.to_string()))?;
    let builder = if cfg.insecure_skip_verify {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAny(provider)))
    } else {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(ca) = &cfg.ca_file {
            for c in load_certs(ca)? {
                roots
                    .add(c)
                    .map_err(|e| err(format!("{}: {e}", ca.display())))?;
            }
        }
        builder.with_root_certificates(roots)
    };
    let mut config = match (&cfg.client_cert, &cfg.client_key) {
        (Some(c), Some(k)) => builder
            .with_client_auth_cert(load_certs(c)?, load_key(k)?)
            .map_err(|e| err(e.to_string()))?,
        _ => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![alpn.to_vec()];
    Ok(Arc::new(config))
}
