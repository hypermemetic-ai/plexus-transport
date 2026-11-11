use std::sync::Arc;

use plexus_core::activations::health::Health;
use plexus_core::plexus::DynamicHub;
use plexus_transport::TransportServer;

pub async fn serve(port: u16) -> anyhow::Result<()> {
    let service = Arc::new(DynamicHub::new("myservice").register(Health::new()));
    TransportServer::builder(service, |service| Ok(DynamicHub::arc_into_rpc_module(service)?))
        .with_websocket(port)
        .build()
        .await?
        .serve()
        .await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    serve(4444).await
}
