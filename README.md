# ipti
Explore and dump [IPLD Prolly Tree Indexes (IPTI)](https://github.com/RangerMauve/ipld-prolly-indexer)

## Installation

Download a binary for your platform from the [latest release](https://github.com/RangerMauve/csv-to-prolly-db/releases/tag/v0.0.3).

## Usage

```bash
NAME:
   ipti - Explore databases in the IPLD Prolly Tree Indexer format

USAGE:
   ipti [global options] command [command options] 

COMMANDS:
   dump     dump a collection into a csv file
   ingest   ingest a collection into a prolly tree from a csv file
   root     Get the root CID from a CAR file
   list     List collections in a CAR
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h  show help
```

## Building

- Set up golang
- clone the repo and open a terminal in it
- run `go build`
- copy `ipti` binary somewhere in your path