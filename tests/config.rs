use aws_config::{Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::SharedCredentialsProvider;
use s3s::{auth::SimpleAuth, host::SingleDomain, service::S3ServiceBuilder};
use sea_orm::Database;
use tele_s3::{backend::Memory, s3::TeleS3};

pub const REGION: &str = "us-east-1";
const DOMAIN_NAME: &str = "localhost:9000";

pub async fn config<const N: usize, const M: usize>() -> anyhow::Result<SdkConfig> {
    let cred = Credentials::for_tests();

    let database = Database::connect("sqlite::memory:").await?;
    let backend = Memory::<N, M>::default();

    let service = {
        let teles3 = TeleS3::init(backend, database).await?;
        let mut service_builder = S3ServiceBuilder::new(teles3);
        service_builder.set_auth(SimpleAuth::from_single(
            cred.access_key_id(),
            cred.secret_access_key(),
        ));
        service_builder.set_host(SingleDomain::new(DOMAIN_NAME)?);
        service_builder.build()
    };

    let client = s3s_aws::Client::from(service);

    let config = SdkConfig::builder()
        .credentials_provider(SharedCredentialsProvider::new(cred))
        .http_client(client)
        .region(Region::new(REGION))
        .endpoint_url(format!("http://{}", DOMAIN_NAME))
        .build();

    Ok(config)
}
