# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| 1.9.x   | ✅        |
| < 1.9   | ❌        |

## Reporting a Vulnerability

Please **do not** open a public issue for security vulnerabilities.

Use GitHub's private vulnerability reporting for this repository:
**Security → Report a vulnerability** (or
<https://github.com/voicetel/memdb/security/advisories/new>).

Include, where possible:

- A description of the issue and its impact
- Steps to reproduce or a proof of concept
- Affected version(s) and configuration

You can expect an acknowledgement within a few business days. Please
allow reasonable time for a fix before any public disclosure.

## Scope Notes

memdb is a Go library that runs SQLite in-memory with snapshot +
WAL durability, optional `hashicorp/raft` replication over mutual
TLS, and an optional PostgreSQL wire-protocol server. Hardening
expectations on the consumer side:

- Treat snapshot files (and any custom `Backend` storage) as
  database contents. Restrict their filesystem permissions to the
  process user; an attacker with snapshot read access has full DB
  read access.
- The PostgreSQL wire-protocol server accepts unauthenticated
  connections by default — bind it to loopback or place behind a
  TLS-terminating proxy / authentication layer before exposing it.
- The Raft transport uses mutual TLS; rotate the CA / leaf certs
  used in `RaftConfig.TLS` and protect their private keys with
  the same care as any other production secret.
- `OnChange` and other panic-safe callbacks recover from panics in
  user code, but logic errors in callbacks can still corrupt
  consumer state. Vet your callbacks.

Out of scope: vulnerabilities in upstream SQLite, in
`hashicorp/raft`, in `pgx` / `lib/pq` (consumer-side); credential
leakage caused by application code that mis-handles connection
strings or snapshot artifacts.
