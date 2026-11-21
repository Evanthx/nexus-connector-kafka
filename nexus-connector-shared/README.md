# nexus-connector-shared

Code every Go connector needs, whatever system it connects to. A connector's own code is about its
system (Kafka, Stripe, Slack, HTTP); everything here is about running as a connector: settings,
connecting to Temporal, encryption, startup, the setup page.

**This folder is the same in every Go connector repo**, apart from the module path in imports. It
will become a module of its own, `github.com/temporalio/nexus-connector-shared`; until then, each
repo has a copy. Moving to the module is then only a change of import path.

## What is here

| Package | What it does | Used by |
|---|---|---|
| `settings` | Declaring and reading environment variables: `Program`, `Variable`, `Env`. The only code that reads the process's environment. Generates the tables in `docs/setup.md`. | Every program |
| `temporalclient` | The Temporal client options every program connects with: `TEMPORAL_*` variables and the profile file, namespace default `connectors`, and the payload codec. The startup report's Temporal section. | Every program |
| `codec` | Encryption: adds the user's payload codec (`TEMPORAL_CODEC_ENDPOINT`, or compiled in) to the client, encrypting payloads and errors, and checks it works at startup. | `temporalclient`, `startup` |
| `startup` | How a program starts: collect every problem before exiting, connect to Temporal, then wait for the payload codec and the program's own system. | Every program's `main` |
| `report` | The startup report's format: settings in sections, defaults marked, then what the program will do. | Every program |
| `deadline` | How long an Outbound Connector's call may take: its limit, ending `CallbackMargin` before the caller's Nexus operation times out. | Outbound Connectors |
| `nexussend` | How an Inbound Connector sends each batch: starts the standalone Nexus operation, retries until it starts, the rate limit, and ordered mode's wait. | Inbound Connectors |
| `configui` | The setup page. Each repo's `configui` command lists its programs and their own checks in `connector.go`. | Each repo's `configui` |
| `test/encryption` | The demo payload codec (AES-256-GCM, `DEMO_ENCRYPTION_KEY`) and its codec server (`go run ./nexus-connector-shared/test/encryption/codecserver`). For demos and tests only. | Demo workers, tests |
| `test/codectest` | Test helpers: a codec server for the demo codec, a client using it, and a check that a payload is encrypted. | End-to-end tests |
| `test/configuitest` | Test helpers for a repo's setup page. | Each repo's `configui` tests |

A connector's `main` reads as its steps, with encryption and the rest handled here:

```go
s := startup.New("Outbound Connector", env)
cfg, err := configFrom(env)
s.Config(err)
opts, c := s.Temporal()
if err := s.Problems(); err != nil {
	log.Fatal(err)
}
defer c.Close()
log.Print(cfg.report(env, opts))
if err := s.Ready(ctx, connectToTheSystem); err != nil { ... }
```

## Changing it

1. Change it in any one repo, with tests here, and check that repo (`make ci`).
2. Copy it to the others, which rewrites the module path:

   ```bash
   create-nexus-connector/scripts/sync-shared.sh <changed repo> <other repo>...
   ```

3. Run `make ci` in each. Check they are all the same with
   `sync-shared.sh --check <repo> <repo>...`.

What goes here: anything a second connector would otherwise copy. What doesn't: anything about one
system, such as Kafka's client, Stripe's API key, or a connector's error types.
