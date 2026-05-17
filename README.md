# ldk-sample
Sample node implementation using LDK.

## Installation
```
git clone https://github.com/lightningdevkit/ldk-sample
```

## Usage
```
cd ldk-sample
cargo run <ldk_storage_directory_path>
```
The only CLI argument is the storage directory path. All configuration is read from
`<ldk_storage_directory_path>/.ldk/config.toml`.

Use `config.example.toml` in the repo root as a template for your config file.

## scorer-kit

`scorer-kit` is an offline developer tool for LDK scorer files. It can inspect,
decode, compare, validate, and merge serialized `ChannelLiquidities` binaries
without starting a node.

Build or run it with:

```sh
cargo run --bin scorer-kit -- --help
```

Common flows:

```sh
# Summarize a scorer binary.
cargo run --bin scorer-kit -- inspect ./scores-z.bin --label source-z

# Decode the full scorer binary to JSON, including historical buckets.
cargo run --bin scorer-kit -- decode ./scores-z.bin \
  --label source-z \
  --save ./source-z.decoded.json

# Compare two scorer binaries.
cargo run --bin scorer-kit -- compare ./scores-z.bin ./scores-b.bin \
  --left-label source-z \
  --right-label source-b

# Merge two scorer binaries using the default richer-history duplicate policy.
cargo run --bin scorer-kit -- merge ./scores-z.bin ./scores-b.bin \
  --label source-z \
  --label source-b \
  --output ./merged.bin \
  --report ./merged.report.json

# Overlay only selected incoming channels onto a baseline scorer binary.
cargo run --bin scorer-kit -- node-scids \
  --graph ./network_graph_cache \
  --node <node-pubkey> \
  --scores ./scores-b.bin \
  --save ./selected.scids
cargo run --bin scorer-kit -- merge ./scores-z.bin ./scores-b.bin \
  --label source-z \
  --label source-b \
  --policy prefer-last \
  --overlay-scids-file ./selected.scids \
  --output ./merged-selected.bin \
  --report ./merged-selected.report.json
```

The default merge policy is `richer-history`: unique entries are preserved, and
duplicate short-channel-id entries keep whichever side has stronger historical
signal. Other policies are available via `--policy prefer-first`,
`--policy prefer-last`, `--policy combine`, and `--policy newer`.

Use `--overlay-scid` / `--overlay-scids-file` when only selected incoming
channels should be merged. The first input is treated as the baseline and is not
filtered; every later input is reduced to the listed short-channel-ids before
merge policies are applied.

Use `node-scids` to derive that allowlist from a serialized LDK `NetworkGraph`.
Scorer binaries only contain short-channel-ids, so node-level selection needs the
graph to map node pubkeys to channel IDs. Pass `--node` directly, or pass
`--invoice` to recover the payee pubkey from a Bolt11 invoice. Passing `--scores`
intersects the graph channels with a scorer file so the allowlist only includes
channels that have incoming score entries.

Decoded JSON scorer snapshots live in `data/scores/`.

## Configuration

Config is loaded from `<storage_dir>/.ldk/config.toml` and strictly validated. Unknown
fields cause an error (`deny_unknown_fields`).

`config.json` is deprecated and no longer supported.

Required sections: `bitcoind`.
Optional sections: `ldk`, `rapid_gossip_sync`, `probing` (probing is disabled if
missing), and `dns_bootstrap` (enabled by default with sensible defaults).

Network options: `mainnet`, `testnet`, `regtest`, `signet` (default is `testnet`).

### Example config

```toml
network = "testnet"

[bitcoind]
rpc_host = "127.0.0.1"
rpc_port = 8332
rpc_username = "your_rpc_user"
rpc_password = "your_rpc_password"

# [ldk]
# peer_listening_port = 9735
# announced_node_name = "MyLDKNode"
# announced_listen_addr = []

[rapid_gossip_sync]
enabled = true
url = "https://rapidsync.lightningdevkit.org/snapshot/"
interval_hours = 6

[probing]
interval_sec = 300
peers = ["02abc123...@1.2.3.4:9735"]
amount_msats = [1000, 10000, 100000, 1000000]
random_min_amount_msat = 1000
random_nodes_per_interval = 1
timeout_sec = 60

probe_delay_sec = 1
peer_delay_sec = 2

[dns_bootstrap]
enabled = true
seeds = [ "nodes.lightning.wiki", "lseed.bitcoinstats.com" ]
timeout_secs = 30
num_peers = 10
interval_secs = 300
```

### Key options

`bitcoind`:
RPC details are required. `rpc_username` and `rpc_password` can usually be found
via `cat ~/.bitcoin/.cookie`.

`ldk`:
`peer_listening_port` defaults to 9735. `announced_listen_addr` and
`announced_node_name` default to empty, disabling public announcements.
`announced_listen_addr` can be set to an IPv4 or IPv6 address to announce a
publicly-connectable address. `announced_node_name` can be any string up to 32
bytes, representing this node's alias.

`rapid_gossip_sync`:
Enabled by default to speed up initial network graph sync. On startup, the node
attempts a RapidGossipSync download up to 3 times with exponential backoff. If it
fails, the node falls back to P2P gossip sync. `url` defaults to
`https://rapidsync.lightningdevkit.org/snapshot/`, `interval_hours` defaults to 6.

`probing`:
Optional. If omitted, probing is disabled. Peer-list probing uses `peers` +
`amount_msats` and probes each configured peer with incrementally increasing amounts.
Random-graph probing uses `random_min_amount_msat` and
`random_nodes_per_interval` to probe randomly selected graph nodes at a fixed
minimal amount each interval. Set `random_min_amount_msat = 0` to disable
random-graph probing.

`dns_bootstrap`:
Optional. Enabled by default. Discovers peers via DNS SRV lookups per BOLT-0010.
`seeds` defaults to `nodes.lightning.wiki` and `lseed.bitcoinstats.com`.
`timeout_secs` defaults to 30, `num_peers` defaults to 10, `interval_secs`
defaults to 300. Set `enabled` to `false` to disable.

## License

Licensed under either:

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
