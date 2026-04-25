use reqwest::StatusCode;

use crate::utils::app::spawn_app;

mod utils;

#[tokio::test]
pub async fn elevations_returns_ok_with_proper_request() {
    let app = spawn_app("tests/fixtures/dem.tif").await;

    let response = app
        .client
        .post(app.url("/elevations"))
        .header("content-type", "application/json")
        .body(r#"[{"lat":42.5,"lon":-111.5}]"#)
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());

    app.task.abort();
}

#[tokio::test]
pub async fn elevations_returns_422_with_wrong_coords() {
    let app = spawn_app("tests/fixtures/dem.tif").await;

    let response = app
        .client
        .post(app.url("/elevations"))
        .header("content-type", "application/json")
        .body(r#"[{"lat":42.5,"lon":-311.5}]"#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    app.task.abort();
}
