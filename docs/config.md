# Configuration

## Concept

It would be neat to have two kinds of configuration: simple and advanced.

Simple config would be what's typically exposed to users.
It would also try to avoid breaking changes.
Advanced config would let users configure basically as much as possible, down to Raft tuning.
There is no consideration given for breaking changes, because it's basically using internal structs.

Internally, simple config could be "compiled" into advanced config.
This compilation would happen in a factory/builder component.
The advanced config is formed of the actual structs that are used by app components.

For the UX, a flag could be provided that lets users provide the advanced config directly.
This would bypass the config compilation.
It's entirely possible, even likely, that advanced config is completely non-functional.
Users are expected to having at least some understanding of the app internals.
That's why it's advanced!

A neat trick would be to enable compiling a simple config into an advanced config, and outputting it to a file.
This would like users use a simple config as a starting point.
