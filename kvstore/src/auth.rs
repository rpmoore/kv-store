use common::auth::{Identity, JwtIssuer, JwtValidator, RsaJwtIssuer, RsaJwtValidator};
use jsonwebtoken::errors::Result;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(crate) struct JwtIssuerVerifier {
    verifier: RsaJwtValidator,
    issuer: RsaJwtIssuer,
}

impl JwtIssuerVerifier {
    pub fn new(private_key: &[u8], public_key_path: &[u8]) -> Result<JwtIssuerVerifier> {
        let issuer = RsaJwtIssuer::new(private_key)?;
        let verifier = RsaJwtValidator::new(public_key_path)?;
        Ok(JwtIssuerVerifier { verifier, issuer })
    }
}

impl JwtValidator for JwtIssuerVerifier {
    fn parse(&self, token_str: impl Into<String>) -> Result<Identity> {
        self.verifier.parse(token_str)
    }
}

impl JwtIssuer for JwtIssuerVerifier {
    fn new_identity(&self, tenant_id: Uuid) -> Result<Identity> {
        self.issuer.new_identity(tenant_id)
    }
}
