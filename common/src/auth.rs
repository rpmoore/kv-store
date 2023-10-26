use std::fmt;
use std::fmt::Formatter;
use uuid::Uuid;
use tracing::{error, instrument};
use jsonwebtoken::{encode, Header, Algorithm, EncodingKey, decode, DecodingKey, errors, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    company: String,
    iss: String,
}

pub struct Identity {
    token: String,
    claims: Claims,
}

impl Identity {
    pub fn tenant_id(self) -> Option<Uuid> {
        let subject = self.claims.sub;
        match Uuid::parse_str(subject.as_str()) {
            Ok(uuid) => Some(uuid),
            Err(err) => {
                error!(err = err.to_string(), "invalid uuid type - should not happen if constructed correctly");
                None
            }
        }
    }

    pub fn token(self) -> String {
        self.token
    }
}

pub trait JwtIssuer {
    fn new_identity(self, tenant_id: Uuid) -> errors::Result<Identity>;
}

#[derive(Clone)]
pub struct RsaJwtIssuer {
    private_key: EncodingKey,
}

impl RsaJwtIssuer {
    pub fn new(rsa_private_key: &[u8]) -> errors::Result<RsaJwtIssuer> { // replace with our own error type
        let private_key = EncodingKey::from_rsa_pem(rsa_private_key)?;

        Ok(RsaJwtIssuer {
            private_key
        })
    }
}

impl JwtIssuer for RsaJwtIssuer {
    #[instrument]
    fn new_identity(self, tenant_id: Uuid) -> errors::Result<Identity> {
        let claims = Claims {
            sub: tenant_id.to_string(),
            company: "my own".to_owned(),
            iss: "kvstore".to_owned(),
        };
        let token = encode(&Header::new(Algorithm::RS256), &claims, &self.private_key)?;

        return Ok(Identity{
            token,
            claims
        })
    }
}

impl fmt::Debug for RsaJwtIssuer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("rsa jwt issuer")
    }
}

pub trait JwtValidator {
    fn parse(self, token_str: &str) -> errors::Result<Identity>;
}

pub struct RsaJwtValidator {
    public_key: DecodingKey
}

impl fmt::Debug for RsaJwtValidator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("rsa jwt validator")
    }
}

impl RsaJwtValidator {
    pub fn new(rsa_public_key: &[u8]) -> errors::Result<RsaJwtValidator> { // replace with our own error type
        let public_key = DecodingKey::from_rsa_pem(rsa_public_key)?;

        Ok(RsaJwtValidator {
            public_key,
        })
    }
}

impl JwtValidator for RsaJwtValidator {
    #[instrument(skip(token_str))]
    fn parse(self, token_str: &str) -> errors::Result<Identity> {
        let token = decode::<Claims>(token_str, &self.public_key, &Validation::new(Algorithm::RS256))?;

        Ok(Identity { token: token_str.to_owned(), claims: token.claims })
    }
}