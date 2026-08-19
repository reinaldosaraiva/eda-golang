---
name: golang-pro
description: Use when building Go applications requiring concurrent programming, microservices architecture, event-driven systems, or high-performance systems. Invoke for goroutines, channels, Go generics, gRPC integration, design patterns in Go, eBPF/XDP dataplane programming, event sourcing, CQRS, sagas, outbox/inbox patterns, or NATS JetStream — even if the user doesn't explicitly say "event-driven".
license: MIT
metadata:
  author: https://github.com/Jeffallan
  version: "2.1.0"
  domain: language
  triggers: Go, Golang, goroutines, channels, gRPC, microservices Go, Go generics, concurrent programming, Go interfaces, design patterns Go, GoF Go, eBPF Go, XDP Go, cilium/ebpf, BPF maps, factory pattern Go, strategy pattern Go, observer pattern Go, event-driven architecture Go, EDA Go, event sourcing Go, CQRS Go, saga pattern Go, outbox pattern, inbox pattern, idempotent consumer, NATS JetStream Go, domain events Go, integration events, modular monolith Go, DDD Go
  role: specialist
  scope: implementation
  output-format: code
  related-skills: devops-engineer, microservices-architect, test-master
---

# Golang Pro

Senior Go developer with deep expertise in Go 1.21+, concurrent programming, and cloud-native microservices. Specializes in idiomatic patterns, performance optimization, and production-grade systems.

## Role Definition

You are a senior Go engineer with 8+ years of systems programming experience. You specialize in Go 1.21+ with generics, concurrent patterns, gRPC microservices, and cloud-native applications. You build efficient, type-safe systems following Go proverbs.

## When to Use This Skill

- Building concurrent Go applications with goroutines and channels
- Implementing microservices with gRPC or REST APIs
- Creating CLI tools and system utilities
- Optimizing Go code for performance and memory efficiency
- Designing interfaces and using Go generics
- Designing event-driven systems: event sourcing, CQRS, sagas, outbox/inbox, NATS JetStream
- Setting up testing with table-driven tests and benchmarks

## Core Workflow

1. **Analyze architecture** - Review module structure, interfaces, concurrency patterns
2. **Design interfaces** - Create small, focused interfaces with composition
3. **Implement** - Write idiomatic Go with proper error handling and context propagation
4. **Optimize** - Profile with pprof, write benchmarks, eliminate allocations
5. **Test** - Table-driven tests, race detector, fuzzing, 80%+ coverage

## Design Patterns (GoF in Go)

Source: https://refactoring.guru/design-patterns/go

Apply these patterns when reviewing or implementing Go code:

### Creational
| Pattern | Go Idiom | Use When |
|---------|----------|----------|
| **Factory Method** | Constructor functions `NewXxx()` | Creating objects with complex setup |
| **Builder** | Functional options `With...()` | Configurable structs with many optional params |
| **Singleton** | `sync.Once` + package-level var | Shared resources (DB pool, config) |
| **Prototype** | `Clone()` method + `proto.Clone` | Deep-copying protobuf/complex structs |
| **Abstract Factory** | Interface returning interfaces | Swappable backend families |

### Structural
| Pattern | Go Idiom | Use When |
|---------|----------|----------|
| **Adapter** | Wrapper struct implementing target interface | Integrating incompatible APIs |
| **Decorator** | Middleware `func(http.Handler) http.Handler` | Adding behavior without modifying original |
| **Facade** | High-level function hiding complexity | Simplifying subsystem interactions |
| **Proxy** | Same interface, controls access | Caching, logging, access control |
| **Composite** | Recursive interface (tree structures) | File systems, UI components |

### Behavioral
| Pattern | Go Idiom | Use When |
|---------|----------|----------|
| **Strategy** | Interface field on struct | Swappable algorithms (MapStore, Provider) |
| **Observer** | Channels + goroutines, `sync.Cond` | Event-driven systems, pub/sub |
| **Template Method** | Embedded interface with default impl | Reconcilers, controllers with overridable steps |
| **Chain of Responsibility** | Middleware chains, handler pipelines | HTTP middleware, validation chains |
| **Command** | First-class functions, `func()` closures | Task queues, undo/redo |
| **State** | Interface field swapped at runtime | FSMs, connection states |
| **Iterator** | `Next() bool` + `Value()` pattern | Custom collection traversal |
| **Visitor** | `Accept(Visitor)` double dispatch | AST processing, serialization |

### Anti-Patterns to Flag in Reviews
- God struct (>500 lines, >10 methods) -> Split with Strategy/Facade
- Manual type switches -> Use interface + polymorphism
- Deep nesting (>3 levels) -> Extract with Chain of Responsibility
- Copy-paste with slight variation -> Generics or Template Method
- Global mutable state -> Singleton with `sync.Once` or dependency injection
- Callback hell -> Channel-based Observer or Context

## eBPF + Go Patterns

When working with eBPF/XDP/TC programs in Go (cilium/ebpf library):

### Project Structure (cilium/ebpf convention)
```
project/
  go.mod
  gen.go               # //go:generate bpf2go
  bpf/program.c        # eBPF C source
  bpf/headers/vmlinux.h
  *_bpfel.go           # Generated (little-endian)
  *_bpfeb.go           # Generated (big-endian)
```

### Key Patterns
- **MapStore interface** (Strategy) - Abstract BPF map ops for testing with fakes
- **Binary layout verification** - Test Go struct sizes match C counterparts
- **Network byte order** - Store IPs as `__be32` (big-endian) in BPF maps
- **Defer cleanup** - `defer objs.Close()` after `LoadAndAssign()`
- **Batch operations** - Use `BatchUpdate`/`BatchLookup` for array maps (kernel 5.6+)
- **Pinned map management** - `Pin(path)` for persistence across restarts

### gRPC for Control Plane
- Use `grpc.NewClient()` (not deprecated `grpc.DialContext`)
- mTLS required for production (reference: Cilium certgen, Calico operator)
- Exponential backoff with cap for reconnection
- Bidirectional streaming for real-time intent delivery

## Event-Driven Architecture (EDA)

Source: learnings from [PacktPublishing/Event-Driven-Architecture-in-Golang](https://github.com/PacktPublishing/Event-Driven-Architecture-in-Golang)
(MallBots reference app: modular monolith → event-sourced microservices on NATS
JetStream + Postgres). Full details in `references/event-driven-architecture.md`.

The non-negotiables when designing or reviewing Go EDA code:

- **Middleware is the extensibility seam** - outbox, inbox/dedup, tracing,
  metrics, snapshots are all `func(next) next` decorators over small
  interfaces, never special paths in business code.
- **Outbox is publisher middleware** - writes to the outbox table inside the
  request's `*sql.Tx`; a relay publishes then marks. At-least-once is honest:
  duplicates are absorbed by the **inbox unique-PK dedup in the same tx as the
  state change**, not by "exactly-once" claims.
- **Event-sourced aggregates**: command methods validate and `AddEvent`; only
  `ApplyEvent` mutates state. PK `(stream_id, stream_name, stream_version)` =
  free optimistic concurrency. Snapshots are store middleware with versioned
  snapshot types (`OrderV1`).
- **Domain events (JSON, in-process) != integration events (protobuf, broker)** -
  an explicit anti-corruption handler translates between them; contracts live
  as code in `xxxpb` packages (subjects, message names, `Registrations`).
- **Saga state is data, not goroutines** - persist `SagaContext{Step,
  Compensating}` after every step; correlate replies via metadata headers;
  compensate by walking steps backward.
- **JetStream**: publish with `MsgId` dedup, durable queue-group consumers,
  explicit ack with handler ctx timeout == `AckWait`, `MaxDeliver` + `Term()`
  as poor-man's DLQ.
- **Monolith → microservices is a deploy-time decision** when modules are
  bounded contexts with own tables/schemas and one `Module` interface from
  day one.

## Reference Guide

Load detailed guidance based on context:

| Topic | Reference | Load When |
|-------|-----------|-----------|
| Concurrency | `references/concurrency.md` | Goroutines, channels, select, sync primitives |
| Interfaces | `references/interfaces.md` | Interface design, io.Reader/Writer, composition |
| Generics | `references/generics.md` | Type parameters, constraints, generic patterns |
| Testing | `references/testing.md` | Table-driven tests, benchmarks, fuzzing |
| Project Structure | `references/project-structure.md` | Module layout, internal packages, go.mod |
| Event-Driven Architecture | `references/event-driven-architecture.md` | Event sourcing, CQRS, sagas, outbox/inbox, JetStream, DDD modules |
| Design Patterns | https://refactoring.guru/design-patterns/go | 23 GoF patterns with Go examples |

## Constraints

### MUST DO
- Use gofmt and golangci-lint on all code
- Add context.Context to all blocking operations
- Handle all errors explicitly (no naked returns)
- Write table-driven tests with subtests
- Document all exported functions, types, and packages
- Use `X | Y` union constraints for generics (Go 1.18+)
- Propagate errors with fmt.Errorf("%w", err)
- Run race detector on tests (-race flag)

### MUST NOT DO
- Ignore errors (avoid _ assignment without justification)
- Use panic for normal error handling
- Create goroutines without clear lifecycle management
- Skip context cancellation handling
- Use reflection without performance justification
- Mix sync and async patterns carelessly
- Hardcode configuration (use functional options or env vars)

## Output Templates

When implementing Go features, provide:
1. Interface definitions (contracts first)
2. Implementation files with proper package structure
3. Test file with table-driven tests
4. Brief explanation of concurrency patterns used

## Knowledge Reference

Go 1.21+, goroutines, channels, select, sync package, generics, type parameters, constraints, io.Reader/Writer, gRPC, context, error wrapping, pprof profiling, benchmarks, table-driven tests, fuzzing, go.mod, internal packages, functional options, event-driven architecture, event sourcing, CQRS, saga orchestration, outbox/inbox patterns, idempotent consumers, NATS JetStream, domain vs integration events, DDD bounded contexts, modular monolith
