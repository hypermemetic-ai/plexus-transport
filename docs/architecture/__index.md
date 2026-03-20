# hub-transport Architecture Documentation

This directory contains architecture decision records and design documents for hub-transport.

## Documents (Reverse Chronological)

- **[16672711854663421183_protocol-methods.md](16672711854663421183_protocol-methods.md)** (2026-03-20)
  - Protocol-level methods architecture
  - Schema discovery and introspection
  - Transport comparison (REST, MCP, WebSocket)
  - Implementation patterns and rationale
  - Auto-generated schema method design
  - Cache validation and backend discovery

- **[16677421176026603519_infrastructure-extraction-pattern.md](16677421176026603519_infrastructure-extraction-pattern.md)** (2026-01-24)
  - How to extract infrastructure into reusable libraries
  - Step-by-step extraction process
  - Lifecycle challenges and solutions
  - Common pitfalls and best practices
  - Checklist for future extractions

- **[16677431963667456511_hub-transport-architecture.md](16677431963667456511_hub-transport-architecture.md)** (2026-01-24)
  - Complete architecture overview
  - Design decisions and rationale
  - RPC conversion pattern
  - Transport module design
  - Migration impact analysis
  - Server naming from activation

## Naming Convention

Architecture documents use reverse-chronological naming to ensure newest documents appear first in alphabetical sorting:

**Format:** `(u64::MAX - nanotime)_title.md`

Where `nanotime` is the current Unix timestamp in nanoseconds. This creates a descending numeric prefix (newer = smaller number = sorts first).

## Quick Reference

**Core Problem:** Extract substrate's transport layer into reusable library
**Key Insight:** Plexus is just an Activation with routing
**Solution:** Generic TransportServer<A: Activation> with callback-based RPC conversion

**Transport Types:**
- stdio (MCP-compatible)
- WebSocket (JSON-RPC)
- MCP HTTP (SSE streaming)
- REST HTTP (RESTful API with HTTP methods)

**Protocol Methods:**
- `schema` - Discovery (all transports)
- `{method}.schema` - Per-method schema (REST, WebSocket)
- `{ns}.call` - Generic invocation (WebSocket only)
- `{ns}.hash` - Cache validation (WebSocket only)
- `_info` - Backend discovery (WebSocket only)

**Design Patterns:**
- Builder pattern for configuration
- Callback for Arc lifecycle preservation
- Generic over Activation trait
- In-memory sessions by default
- Protocol methods as first-class methods
- Strongly-typed HTTP method routing
