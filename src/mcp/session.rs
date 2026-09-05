//! SQLite-backed MCP session manager for persistent sessions across restarts
//!
//! This module provides a SessionManager implementation that persists session
//! state to SQLite, allowing clients to reconnect after server restarts.
//!
//! Sessions older than 30 days (configurable) are automatically cleaned up on startup.

use std::{
    collections::HashMap,
    path::PathBuf,
    time::Duration,
};

use futures::Stream;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePool},
    ConnectOptions,
};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;

use rmcp::{
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::{
        WorkerTransport,
        common::server_side_http::{SessionId, ServerSseMessage, session_id},
        streamable_http_server::session::{
            SessionManager,
            local::{
                LocalSessionWorker, LocalSessionHandle, SessionConfig,
                SessionError, create_local_session, EventIdParseError,
            },
        },
    },
};

/// Default session cleanup age: 30 days
pub const DEFAULT_SESSION_MAX_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Configuration for SQLite session storage
#[derive(Debug, Clone)]
pub struct SqliteSessionConfig {
    /// Path to SQLite database
    pub db_path: PathBuf,
    /// Session worker configuration
    pub session_config: SessionConfig,
    /// Maximum age for sessions before cleanup (default: 30 days)
    pub max_session_age: Duration,
}

impl Default for SqliteSessionConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("mcp_sessions.db"),
            session_config: SessionConfig::default(),
            max_session_age: DEFAULT_SESSION_MAX_AGE,
        }
    }
}

/// Error types for SQLite session manager
#[derive(Debug, Error)]
pub enum SqliteSessionError {
    #[error("Session not found: {0}")]
    SessionNotFound(SessionId),
    #[error("Session error: {0}")]
    SessionError(#[from] SessionError),
    #[error("Invalid event id: {0}")]
    InvalidEventId(#[from] EventIdParseError),
    #[error("Database error: {0}")]
    DatabaseError(String),
}

/// SQLite-backed session manager
///
/// Persists session IDs to SQLite so clients can reconnect after server restart.
/// The actual session workers are created on-demand, but session identity persists.
pub struct SqliteSessionManager {
    pool: SqlitePool,
    /// In-memory session handles (runtime state)
    sessions: RwLock<HashMap<SessionId, LocalSessionHandle>>,
    session_config: SessionConfig,
    /// Maximum age for sessions before cleanup
    max_session_age: Duration,
}

impl SqliteSessionManager {
    /// Create a new SQLite session manager
    pub async fn new(config: SqliteSessionConfig) -> Result<Self, SqliteSessionError> {
        let db_url = format!("sqlite:{}?mode=rwc", config.db_path.display());
        let connect_options: SqliteConnectOptions = db_url
            .parse()
            .map_err(|e| SqliteSessionError::DatabaseError(format!("Failed to parse DB URL: {}", e)))?;
        // sqlx 0.8: disable_statement_logging takes self by value and returns Self
        let connect_options = connect_options.disable_statement_logging();

        let pool = SqlitePool::connect_with(connect_options)
            .await
            .map_err(|e| SqliteSessionError::DatabaseError(format!("Failed to connect: {}", e)))?;

        let manager = Self {
            pool,
            sessions: RwLock::new(HashMap::new()),
            session_config: config.session_config,
            max_session_age: config.max_session_age,
        };

        manager.run_migrations().await?;

        // Clean up old sessions on startup
        let cleaned = manager.cleanup_old_sessions().await?;
        if cleaned > 0 {
            tracing::info!(count = cleaned, "Cleaned up old MCP sessions");
        }

        // Log persisted sessions (for debugging)
        let persisted = manager.count_persisted_sessions().await?;
        if persisted > 0 {
            tracing::info!(
                count = persisted,
                "Found persisted MCP sessions (clients will need to reconnect)"
            );
        }

        Ok(manager)
    }

    /// Count persisted sessions in database
    async fn count_persisted_sessions(&self) -> Result<usize, SqliteSessionError> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM mcp_sessions")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SqliteSessionError::DatabaseError(format!("Failed to count sessions: {}", e)))?;

        let count: i64 = sqlx::Row::get(&row, "count");
        Ok(count as usize)
    }


    /// Clean up sessions older than max_session_age
    ///
    /// Returns the number of sessions cleaned up
    pub async fn cleanup_old_sessions(&self) -> Result<usize, SqliteSessionError> {
        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - self.max_session_age.as_secs() as i64;

        let result = sqlx::query("DELETE FROM mcp_sessions WHERE last_seen_at < ?")
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .map_err(|e| SqliteSessionError::DatabaseError(format!("Failed to cleanup sessions: {}", e)))?;

        Ok(result.rows_affected() as usize)
    }

    /// Run database migrations
    async fn run_migrations(&self) -> Result<(), SqliteSessionError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS mcp_sessions (
                id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS mcp_session_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY (session_id) REFERENCES mcp_sessions(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_session_cache_session ON mcp_session_cache(session_id);
            CREATE INDEX IF NOT EXISTS idx_session_cache_event ON mcp_session_cache(session_id, event_id);
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SqliteSessionError::DatabaseError(format!("Migration failed: {}", e)))?;

        Ok(())
    }

    /// Record a session in the database
    async fn persist_session(&self, id: &SessionId) -> Result<(), SqliteSessionError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        sqlx::query(
            "INSERT OR REPLACE INTO mcp_sessions (id, created_at, last_seen_at) VALUES (?, ?, ?)",
        )
        .bind(id.as_ref())
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| SqliteSessionError::DatabaseError(format!("Failed to persist session: {}", e)))?;

        Ok(())
    }

    /// Update last seen timestamp
    async fn touch_session(&self, id: &SessionId) -> Result<(), SqliteSessionError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        sqlx::query("UPDATE mcp_sessions SET last_seen_at = ? WHERE id = ?")
            .bind(now)
            .bind(id.as_ref())
            .execute(&self.pool)
            .await
            .map_err(|e| SqliteSessionError::DatabaseError(format!("Failed to touch session: {}", e)))?;

        Ok(())
    }

    /// Check if a session exists in the database
    async fn session_exists_in_db(&self, id: &SessionId) -> Result<bool, SqliteSessionError> {
        let row = sqlx::query("SELECT 1 FROM mcp_sessions WHERE id = ?")
            .bind(id.as_ref())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SqliteSessionError::DatabaseError(format!("Failed to check session: {}", e)))?;

        Ok(row.is_some())
    }

    /// Remove a session from the database
    async fn remove_session_from_db(&self, id: &SessionId) -> Result<(), SqliteSessionError> {
        sqlx::query("DELETE FROM mcp_sessions WHERE id = ?")
            .bind(id.as_ref())
            .execute(&self.pool)
            .await
            .map_err(|e| SqliteSessionError::DatabaseError(format!("Failed to remove session: {}", e)))?;

        Ok(())
    }

    /// Recreate a session worker for a known session ID (for reconnection after restart)
    async fn recreate_session(
        &self,
        id: SessionId,
    ) -> Result<WorkerTransport<LocalSessionWorker>, SqliteSessionError> {
        let (handle, worker) = create_local_session(id.clone(), self.session_config.clone());
        self.sessions.write().await.insert(id.clone(), handle);
        self.touch_session(&id).await?;
        Ok(WorkerTransport::spawn(worker))
    }
}

impl SessionManager for SqliteSessionManager {
    type Error = SqliteSessionError;
    type Transport = WorkerTransport<LocalSessionWorker>;

    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        let id = session_id();
        let (handle, worker) = create_local_session(id.clone(), self.session_config.clone());

        // Persist to database
        self.persist_session(&id).await?;

        // Store in memory
        self.sessions.write().await.insert(id.clone(), handle);

        tracing::info!(session_id = ?id, "Created new persistent MCP session");
        Ok((id, WorkerTransport::spawn(worker)))
    }

    async fn initialize_session(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        // Check if session exists in memory
        let sessions = self.sessions.read().await;
        if let Some(handle) = sessions.get(id) {
            let response = handle.initialize(message).await?;
            return Ok(response);
        }
        drop(sessions);

        // Check if session exists in database (reconnection case)
        if self.session_exists_in_db(id).await? {
            tracing::info!(session_id = ?id, "Reconnecting to persisted MCP session");
            // Note: For reconnection, the transport would need to be re-established
            // This is a limitation - we can track the session but can't fully restore state
            return Err(SqliteSessionError::SessionNotFound(id.clone()));
        }

        Err(SqliteSessionError::SessionNotFound(id.clone()))
    }

    async fn has_session(&self, id: &SessionId) -> Result<bool, Self::Error> {
        // Only return true if the session worker is active in memory
        // Workers can't be restored without handler connection (rmcp limitation)
        if self.sessions.read().await.contains_key(id) {
            return Ok(true);
        }

        // Session in DB but no active worker - remove stale entry and return false
        // Client will get 401 and should reconnect with fresh session
        if self.session_exists_in_db(id).await? {
            tracing::info!(session_id = ?id, "Removing stale session from DB (no active worker)");
            self.remove_session_from_db(id).await.ok();
        }

        Ok(false)
    }

    async fn close_session(&self, id: &SessionId) -> Result<(), Self::Error> {
        // Remove from memory
        let mut sessions = self.sessions.write().await;
        if let Some(handle) = sessions.remove(id) {
            handle.close().await?;
        }

        // Remove from database
        self.remove_session_from_db(id).await?;

        tracing::info!(session_id = ?id, "Closed MCP session");
        Ok(())
    }

    async fn create_stream(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + 'static, Self::Error> {
        let sessions = self.sessions.read().await;
        let handle = sessions
            .get(id)
            .ok_or(SqliteSessionError::SessionNotFound(id.clone()))?;

        let receiver = handle.establish_request_wise_channel().await?;
        handle
            .push_message(message, receiver.http_request_id)
            .await?;

        self.touch_session(id).await.ok(); // Best effort
        Ok(ReceiverStream::new(receiver.inner))
    }

    async fn create_standalone_stream(
        &self,
        id: &SessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + 'static, Self::Error> {
        let sessions = self.sessions.read().await;
        let handle = sessions
            .get(id)
            .ok_or(SqliteSessionError::SessionNotFound(id.clone()))?;

        let receiver = handle.establish_common_channel().await?;
        self.touch_session(id).await.ok(); // Best effort
        Ok(ReceiverStream::new(receiver.inner))
    }

    async fn resume(
        &self,
        id: &SessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + 'static, Self::Error> {
        // Check memory first
        {
            let sessions = self.sessions.read().await;
            if let Some(handle) = sessions.get(id) {
                let receiver = handle.resume(last_event_id.parse()?).await?;
                self.touch_session(id).await.ok();
                return Ok(ReceiverStream::new(receiver.inner));
            }
        }

        // Check if this is a reconnection after restart
        if self.session_exists_in_db(id).await? {
            tracing::info!(session_id = ?id, last_event_id, "Session reconnection attempt - recreating worker");
            // Recreate the session worker
            let _transport = self.recreate_session(id.clone()).await?;

            // Now try to get the handle and resume
            let sessions = self.sessions.read().await;
            if let Some(handle) = sessions.get(id) {
                let receiver = handle.resume(last_event_id.parse()?).await?;
                return Ok(ReceiverStream::new(receiver.inner));
            }
        }

        Err(SqliteSessionError::SessionNotFound(id.clone()))
    }

    async fn accept_message(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        let sessions = self.sessions.read().await;
        let handle = sessions
            .get(id)
            .ok_or(SqliteSessionError::SessionNotFound(id.clone()))?;

        handle.push_message(message, None).await?;
        self.touch_session(id).await.ok(); // Best effort
        Ok(())
    }
}
