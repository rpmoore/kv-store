pub mod healthcheck;

pub mod storage {
    tonic::include_proto!("storage"); // The string specified here must match the proto package name
}
/*
pub mod admin {
    tonic::include_proto!("admin");
}
 */
