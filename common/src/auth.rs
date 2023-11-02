use std::collections::HashSet;
use std::fmt;
use std::fmt::{Display, Formatter, Write};
use std::io::ErrorKind;
use actix_web::error::ParseError;
use actix_web::http::header;
use actix_web::http::header::{HeaderName, HeaderValue, InvalidHeaderValue, TryIntoHeaderValue};
use actix_web::HttpMessage;
use uuid::Uuid;
use std::sync::Arc;
use sha2::{Sha384, Digest};
use base64::{Engine as _, engine::general_purpose};
use tracing::{error, instrument};
use jsonwebtoken::{encode, Header, Algorithm, EncodingKey, decode, DecodingKey, errors, Validation};
use serde::{Deserialize, Serialize, Serializer};
use tonic::metadata::{MetadataMap, MetadataValue};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: Uuid,
    company: String,
    iss: String,
}

#[derive(Clone, Debug)]
pub struct Token(Arc<str>);

impl Serialize for Token {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        serializer.serialize_str(self.as_ref())
    }
}

impl AsRef<str> for Token {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

// Implement Display so that we can hash the token and we don't accidentally store it in logs
impl Display for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut hasher = Sha384::new();
        f.write_str("sha384::")?;
        hasher.update(self.0.as_bytes());
        let result = hasher.finalize();
        f.write_str(general_purpose::STANDARD_NO_PAD.encode(result).as_str())?;
        Ok(())
    }
}

pub struct Identity {
    token: Token,
    claims: Claims,
}

impl Identity {
    pub fn tenant_id(&self) -> Uuid {
        self.claims.sub
    }

    pub fn token(&self) -> Token {
        self.token.clone()
    }
}

pub trait JwtIssuer {
    fn new_identity(&self, tenant_id: Uuid) -> errors::Result<Identity>;
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
    fn new_identity(&self, tenant_id: Uuid) -> errors::Result<Identity> {
        let claims = Claims {
            sub: tenant_id,
            company: "my own".to_owned(),
            iss: "kvstore".to_owned(),

        };
        let token = encode(&Header::new(Algorithm::RS256), &claims, &self.private_key)?;

        return Ok(Identity{
            token: Token(token.into()),
            claims,
        })
    }
}

impl fmt::Debug for RsaJwtIssuer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("rsa jwt issuer")
    }
}

pub trait JwtValidator {
    fn parse(&self, token_str: impl Into<String>) -> errors::Result<Identity>;
}

#[derive(Clone)]
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
    fn parse(&self, token_str: impl Into<String>) -> errors::Result<Identity> {
        let token_str = token_str.into();
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = false; // TODO for production remove this
        validation.required_spec_claims = HashSet::new();

        let token = decode::<Claims>(&token_str, &self.public_key, &validation)?;

        Ok(Identity { token: Token(token_str.into()), claims: token.claims})
    }
}

pub struct AuthHeader {
    bearer: String
}

impl From<AuthHeader> for String {
    fn from(value: AuthHeader) -> Self {
        value.bearer
    }
}

impl AsRef<str> for AuthHeader {
    fn as_ref(&self) -> &str {
        self.bearer.as_str()
    }
}

impl TryFrom<&MetadataMap> for AuthHeader {
    type Error = ErrorKind;

    fn try_from(value: &MetadataMap) -> Result<Self, Self::Error> {
        value.get("Authorization")
            .ok_or(ErrorKind::NotFound)
            .and_then(|header| header.to_str().map_err(|err|{
                error!(err = err.to_string(), "failed to get auth header");
                ErrorKind::NotFound
            } ))
            .and_then(|auth|auth.split_ascii_whitespace()
                .nth(1)
                .ok_or(ErrorKind::NotFound)
            ).map(|token| AuthHeader{bearer:token.to_string()})
    }
}

impl From<AuthHeader> for MetadataMap {
    fn from(header: AuthHeader) -> Self {
        let mut map = MetadataMap::new();

        match MetadataValue::try_from(format!("Bearer {}", header.bearer)) {
            Ok(value) => {
                map.append(header::AUTHORIZATION.as_str(), value);
                map
            },
            Err(err) => {
                error!(err = err.to_string(), "failed to append authorization header");
                map
            }
        }
    }
}

impl fmt::Debug for AuthHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("auth header")
    }
}

impl TryIntoHeaderValue for AuthHeader {
    type Error = InvalidHeaderValue;

    fn try_into_value(self) -> Result<HeaderValue, Self::Error> {
        HeaderValue::try_from(format!("Bearer {}", self.bearer))
    }
}

impl header::Header for AuthHeader {
    fn name() -> HeaderName {
        header::AUTHORIZATION
    }

    fn parse<M: HttpMessage>(msg: &M) -> Result<Self, ParseError> {
        match msg.headers().get(header::AUTHORIZATION) {
            Some(auth_header) => match auth_header.to_str().map_err(|err| {
                error!(err = err.to_string(), "failed to get auth header");
                ParseError::Header
            }
            ).and_then(|value| value.split_ascii_whitespace().nth(1).ok_or(ParseError::Header)) {
                Ok(auth) => Ok(AuthHeader{bearer: String::from(auth)}),
                Err(err)=> {
                    error!{err = err.to_string(), "failed to get auth header"}
                    Err(ParseError::Header)
                }
            },
            None => Err(ParseError::Header)
        }
    }
}