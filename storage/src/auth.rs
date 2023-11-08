use tonic::{Code, Request, Status};
use tonic::service::Interceptor;
use tracing::{error, info};
use common::auth::{JwtValidator, RsaJwtValidator};

#[derive(Debug, Clone)]
pub struct AuthInterceptor {
    jwt_validator: RsaJwtValidator,
}

impl AuthInterceptor {
    pub fn new(jwt_validator: RsaJwtValidator) -> AuthInterceptor {
        AuthInterceptor { jwt_validator }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let Ok(auth_header) = common::auth::AuthHeader::try_from(request.metadata()) else {
            error!("invalid auth header");
            return Err(Status::new(Code::Unauthenticated, "auth header missing"));
        };

        let Ok(identity) = self.jwt_validator.parse(auth_header) else {
            error!("invalid auth header");
            return Err(Status::new(Code::NotFound, "not found"));
        };

        info!(
            tenant_id = identity.tenant_id().to_string(),
            "authenticated as tenant"
        );
        request.extensions_mut().insert(identity);
        Ok(request)
    }
}
