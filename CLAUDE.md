# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Flink — a distributed stream processing framework supporting both streaming and batch processing. Version 2.3-SNAPSHOT, multi-module Maven project with 40+ modules.

## Build Commands

```bash
# Build (skip tests) — default Java 17 target
./mvnw clean package -DskipTests

# Build targeting specific Java versions
./mvnw clean package -DskipTests -Djdk11 -Pjava11-target
./mvnw clean package -DskipTests -Djdk17 -Pjava17-target
./mvnw clean package -DskipTests -Djdk21 -Pjava21-target

# Fast build (skips checkstyle, spotless, RAT, enforcer, javadoc)
./mvnw clean package -DskipTests -Dfast

# Build a single module (e.g., flink-runtime)
./mvnw clean package -DskipTests -pl flink-runtime -am

# Run all tests
./mvnw clean verify

# Run a single test class
./mvnw test -pl flink-runtime -Dtest=TaskExecutorTest

# Run a single test method
./mvnw test -pl flink-runtime -Dtest=TaskExecutorTest#testMethodName

# Apply code formatting (Java + Scala)
./mvnw spotless:apply

# Check code formatting without applying
./mvnw spotless:check

# Run checkstyle
./mvnw checkstyle:check
```

Build output goes to `build-target/`.

## Code Style

**Java:** Spotless + google-java-format v1.24.0.0. 4-space indent, 8-space continuation, 100-char line limit. Binary operators wrap to next line. Always run `./mvnw spotless:apply` before committing.

**Scala:** scalafmt v3.4.3 (config in `.scalafmt.conf`). 100-char line limit.

**Import order** (both Java and Scala):
1. `org.apache.flink.*`
2. `org.apache.flink.shaded.*`
3. `*` (everything else)
4. `javax.*`
5. `java.*`
6. `scala.*`

No wildcard imports (import-on-demand threshold set to 9999).

**XML files:** Tab indentation (4-space width). Exception: `*Test.xml` files use 2-space indentation.

## Key Conventions

- **Shaded dependencies:** Never import Guava, Jackson (codehaus), Netty, or ASM directly. Use their `flink-shaded-*` equivalents.
- **No `Throwables.propagate()`** — it's deprecated.
- **No Commons Lang v1** — use v3 (`org.apache.commons.lang3`).
- **No `Boolean.getBoolean()`, `Integer.getInteger()`, `Long.getLong()`** — these read system properties, not parse strings. Use `System.getProperties()` if that's the intent.
- **No `TODO(username)` comments** — use plain `TODO` without attribution.
- **Javadoc required** on `protected` and `public` members.
- **All files:** UTF-8, LF line endings, trailing newline.
- **License header:** Apache 2.0 license header required on all source files.

## Architecture

**API Layer:**
- `flink-core-api` / `flink-core` — Core abstractions, serialization, type system
- `flink-datastream-api` / `flink-datastream` — DataStream API for streaming programs
- `flink-table` — Table API and SQL support

**Runtime Layer:**
- `flink-runtime` — Execution engine, scheduler, state management, network, memory management
- `flink-rpc` — RPC framework for distributed communication
- `flink-clients` — Job submission client

**State & Storage:**
- `flink-state-backends` — State backend implementations (RocksDB, etc.)
- `flink-queryable-state` — External state query API
- `flink-dstl` — Distributed Stream Table Library (disaggregated state)
- `flink-filesystems` — File system abstraction (S3, HDFS, etc.)

**Connectors & Formats:**
- `flink-connectors` — Base connector infrastructure (most connectors now in separate repos)
- `flink-formats` — Data format handlers (Avro, Parquet, ORC, etc.)

**Deployment:**
- `flink-kubernetes` — Kubernetes integration
- `flink-yarn` — YARN cluster integration
- `flink-dist` / `flink-dist-scala` — Distribution packaging

**Libraries:**
- `flink-libraries` — Graph (Gelly), ML, CEP

**Testing:**
- `flink-test-utils-parent` — Shared test utilities
- `flink-architecture-tests` — ArchUnit-based architectural rule enforcement
- `flink-tests` — Core integration tests
- `flink-end-to-end-tests` — E2E test suites

## Architecture Tests

`flink-architecture-tests` uses ArchUnit to enforce structural rules. Scala classes are excluded. Violations are "frozen" — existing violations are recorded and new ones fail the build. New rules must be wrapped in `FreezingArchRule.freeze()` with fixed descriptions. To regenerate violation stores after adding a rule:

```bash
rm -rf $(find . -type d -name archunit-violations) && \
./mvnw test -Dtest="*TestCodeArchitectureTest*" -DfailIfNoTests=false \
  -Darchunit.freeze.refreeze=true \
  -Darchunit.freeze.store.default.allowStoreCreation=true
```

## Test Configuration

- Unit test heap: 768m, Integration test heap: 1536m, Max: 3072m
- GC: G1GC for all tests
- Unit test fork count: 4, Integration test fork count: 2
- Test name patterns: `**/*Test.*` (unit), `**/*ITCase.*` (integration)
- Randomization: `./mvnw test -Dtest.randomization.seed=<seed>`

## PR and Commit Conventions

- PRs must reference a JIRA issue: `[FLINK-XXXX][component] Title`
- Exception: `[hotfix]` prefix for trivial typo fixes
- Merge strategy: squash or rebase (merge commits disabled)
- Run `./mvnw clean verify` before submitting
