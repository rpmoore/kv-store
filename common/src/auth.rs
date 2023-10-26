use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;
use std::io::ErrorKind;
use actix_web::error::ParseError;
use actix_web::http::header;
use actix_web::http::header::{HeaderName, HeaderValue, InvalidHeaderValue, TryIntoHeaderValue};
use actix_web::HttpMessage;
use uuid::Uuid;
use tracing::{error, instrument};
use jsonwebtoken::{encode, Header, Algorithm, EncodingKey, decode, DecodingKey, errors, Validation};
use serde::{Deserialize, Serialize};
use tonic::metadata::{MetadataMap, MetadataValue};

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
    fn parse(self, token_str: &str) -> errors::Result<Identity> {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = false; // TODO for production remove this
        validation.required_spec_claims = HashSet::new();

        let token = decode::<Claims>(token_str, &self.public_key, &validation)?;

        Ok(Identity { token: token_str.to_owned(), claims: token.claims })
    }
}

pub struct AuthHeader {
    bearer: String
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
            .and_then(|header| header.to_str().map_err(|err| ErrorKind::NotFound))
            .and_then(|auth|auth.split_ascii_whitespace()
                .skip(1)
                .next()
                .ok_or(ErrorKind::NotFound)
            ).map(|token| AuthHeader{bearer:token.to_string()})
    }
}

impl Into<MetadataMap> for AuthHeader{
    fn into(self) -> MetadataMap {
        let mut map = MetadataMap::new();

        match MetadataValue::try_from(format!("Bearer {}", self.bearer)) {
            Ok(value) => {
                map.append(header::AUTHORIZATION.as_str(), value);
                map
            },
            Err(err) => {
                error!("failed to append authorization header");
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
            ).and_then(|value| value.split_ascii_whitespace().skip(1).next().ok_or(ParseError::Header)) {
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