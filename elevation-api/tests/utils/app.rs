use std::{net::TcpListener, path::Path};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct TestApp {
    pub address: String,
    pub task: JoinHandle<()>,
    pub client: reqwest::Client,
}

impl TestApp {
    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.address, path)
    }
}

pub async fn spawn_app(fixture_geotiff: impl AsRef<Path>) -> TestApp {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let storage_dir = tempfile::tempdir().unwrap();
    let file_to_ingest = storage_dir.path().join("input.tif");
    tokio::fs::copy(fixture_geotiff.as_ref(), &file_to_ingest)
        .await
        .unwrap();

    let metadata_registry_name = format!("test-{}", Uuid::new_v4());

    let app_addr = format!("127.0.0.1:{port}").parse().unwrap();
    let storage_dir_path = storage_dir.path().to_path_buf();
    let file_to_ingest_path = file_to_ingest.clone();
    let metadata_registry_name_for_task = metadata_registry_name.clone();

    let task = tokio::spawn(async move {
        elevation_api::run(
            app_addr,
            storage_dir_path,
            file_to_ingest_path,
            metadata_registry_name_for_task,
        )
        .await
        .unwrap();
    });

    let app = TestApp {
        address: format!("http://127.0.0.1:{port}"),
        task,
        client: reqwest::Client::new(),
    };

    wait_until_ready(&app).await;

    app
}

async fn wait_until_ready(app: &TestApp) {
    let mut attempts = 0usize;

    loop {
        attempts += 1;

        let response = app
            .client
            .post(app.url("/elevations"))
            .header("content-type", "application/json")
            .body("[]")
            .send()
            .await;

        if response.is_ok() {
            break;
        }

        if attempts > 50 {
            panic!("application did not start in time");
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
