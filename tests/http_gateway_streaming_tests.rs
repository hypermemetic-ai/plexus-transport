//! Tests for HTTP gateway streaming vulnerabilities and fixes
//!
//! These tests demonstrate and verify protection against:
//! - Infinite streams (never emit Done)
//! - Memory exhaustion (unbounded buffering)
//! - Timeouts for long-running operations
//! - SSE stream completion

#[cfg(feature = "http-gateway")]
mod http_streaming_tests {
    use async_stream::stream;
    use plexus_core::plexus::{
        types::{PlexusStreamItem, StreamMetadata},
        PlexusStream,
    };
    use plexus_transport::http::handler::{handle_method_call, MethodInfo};
    use serde_json::json;
    use std::time::Duration;
    use axum::http::StatusCode;
    use axum::body;

    // =========================================================================
    // Test Stream Generators
    // =========================================================================

    fn metadata() -> StreamMetadata {
        StreamMetadata::new(vec!["test".into()], "hash123".into())
    }

    fn create_infinite_stream() -> PlexusStream {
        Box::pin(stream! {
                        // Emit a few items then hang forever
                        for i in 0..5 {
                            yield PlexusStreamItem::Data {
                                metadata: metadata(),
                                content_type: "test".into(),
                                content: json!({"item": i}),
                            };
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
            // Never yield Done - just hang
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        })
    }

    fn create_memory_exhaustion_stream(item_count: usize, item_size_kb: usize) -> PlexusStream {
        Box::pin(stream! {
            let large_data = vec![0u8; item_size_kb * 1024];
            for i in 0..item_count {
                yield PlexusStreamItem::Data {
                    metadata: metadata(),
                    content_type: "test".into(),
                    content: json!({
                        "item": i,
                        "data": large_data.clone()
                    }),
                };
            }
            yield PlexusStreamItem::Done { metadata: metadata() };
        })
    }

    fn create_slow_completion_stream(delay_ms: u64, item_count: usize) -> PlexusStream {
        Box::pin(stream! {
            for i in 0..item_count {
                yield PlexusStreamItem::Data {
                    metadata: metadata(),
                    content_type: "test".into(),
                    content: json!({"item": i}),
                };
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            yield PlexusStreamItem::Done { metadata: metadata() };
        })
    }

    fn create_normal_stream() -> PlexusStream {
        Box::pin(stream! {
            yield PlexusStreamItem::Data {
                metadata: metadata(),
                content_type: "test".into(),
                content: json!({"result": "success"}),
            };
            yield PlexusStreamItem::Done { metadata: metadata() };
        })
    }

    fn create_method_info(streaming: bool) -> MethodInfo {
        MethodInfo {
            namespace: "test".to_string(),
            method: "test_method".to_string(),
            streaming,
        }
    }

    // =========================================================================
    // Tests - Infinite Stream Vulnerability
    // =========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_infinite_stream_hangs_without_timeout() {
        // This test demonstrates the vulnerability: it will hang forever
        // We use a timeout on the test itself to prevent CI from hanging

        let stream = create_infinite_stream();
        let method_info = create_method_info(false); // Non-streaming method

        let handler_future = handle_method_call(stream, method_info);

        // This should timeout because the handler hangs forever
        let result = tokio::time::timeout(Duration::from_secs(2), handler_future).await;

        // EXPECTED TO FAIL: The request hangs and times out
        assert!(
            result.is_err(),
            "Request should timeout because stream never completes (this demonstrates the bug)"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_infinite_stream_protected_by_timeout() {
        // After fix: handler should timeout gracefully

        let stream = create_infinite_stream();
        let method_info = create_method_info(false);

        let response = handle_method_call(stream, method_info).await;

        // SHOULD PASS AFTER FIX: Handler returns timeout error
        assert_eq!(
            response.status(),
            StatusCode::GATEWAY_TIMEOUT,
            "Handler should return 504 Gateway Timeout for infinite streams"
        );

        let body_bytes = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert!(
            json.get("error").is_some(),
            "Response should contain error message"
        );
    }

    // =========================================================================
    // Tests - Memory Exhaustion Vulnerability
    // =========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_memory_exhaustion_unbounded_buffering() {
        // This test was designed to demonstrate the vulnerability
        // Now that we've added protection, it correctly rejects the stream
        // Note: This is essentially the same as test_memory_exhaustion_protected_by_limits
        // but kept to show the before/after behavior

        let stream = create_memory_exhaustion_stream(1000, 100);
        let method_info = create_method_info(false);

        let handler_future = handle_method_call(stream, method_info);

        // Give it reasonable time to process
        let result = tokio::time::timeout(Duration::from_secs(10), handler_future).await;

        if result.is_ok() {
            let response = result.unwrap();
            // After fix: this now returns 413 Payload Too Large (protection working!)
            assert_eq!(
                response.status(),
                StatusCode::PAYLOAD_TOO_LARGE,
                "With protection, this correctly rejects 100MB stream"
            );
        } else {
            panic!("Request timed out - this could indicate the test environment is too slow");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_memory_exhaustion_protected_by_limits() {
        // After fix: should reject with 413 Payload Too Large

        let stream = create_memory_exhaustion_stream(1000, 100);
        let method_info = create_method_info(false);

        let response = handle_method_call(stream, method_info).await;

        // SHOULD PASS AFTER FIX: Handler returns payload too large
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "Handler should return 413 Payload Too Large when buffer limits exceeded"
        );

        let body_bytes = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert!(
            json.get("error").is_some(),
            "Response should contain error message"
        );
    }

    // =========================================================================
    // Tests - Reasonable Requests Should Work
    // =========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_normal_request_completes_successfully() {
        let stream = create_normal_stream();
        let method_info = create_method_info(false);

        let response = handle_method_call(stream, method_info).await;

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "Normal requests should complete successfully"
        );

        let body_bytes = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert!(json.get("data").is_some(), "Response should contain data");
        assert_eq!(
            json["data"].as_array().unwrap().len(),
            1,
            "Should have one data item"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_slow_completion_within_timeout() {
        // Request that takes ~1 second but completes within timeout
        let stream = create_slow_completion_stream(100, 10);
        let method_info = create_method_info(false);

        let response = handle_method_call(stream, method_info).await;

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "Slow requests within timeout should complete successfully"
        );

        let body_bytes = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(
            json["data"].as_array().unwrap().len(),
            10,
            "Should have all 10 items"
        );
    }

    // =========================================================================
    // Tests - Buffer Limit Edge Cases
    // =========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_limit_by_item_count() {
        // Test that item count limit is enforced (not just byte limit)
        // After fix: should reject when too many items regardless of size

        let stream = create_memory_exhaustion_stream(20_000, 1);
        let method_info = create_method_info(false);

        let response = handle_method_call(stream, method_info).await;

        // SHOULD PASS AFTER FIX
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "Should reject when item count exceeds limit"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_limit_by_total_bytes() {
        // Test that total byte limit is enforced

        let stream = create_memory_exhaustion_stream(100, 2000);
        let method_info = create_method_info(false);

        let response = handle_method_call(stream, method_info).await;

        // SHOULD PASS AFTER FIX
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "Should reject when total bytes exceeds limit"
        );
    }
}
