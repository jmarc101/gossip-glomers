# Fly.io Distributed Systems Challenge

Solutions to the [Fly.io Distributed Systems Challenge](https://fly.io/dist-sys/) using Go and Maelstrom.

## Setup

### Install Dependencies

**Ubuntu/Debian:**
```bash
sudo apt install openjdk-17-jdk graphviz gnuplot golang
```

**macOS:**
```bash
brew install openjdk@17 graphviz gnuplot go
```

**Windows:** Use WSL2 and follow Ubuntu instructions.

### Install Maelstrom

```bash
wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2
tar -xvjf maelstrom.tar.bz2
export PATH=$PATH:$(pwd)/maelstrom
```

## Usage

### Build and Test
```bash
cd <challenge-folder>
go build .
maelstrom test -w <workload> --bin ./<binary> [options]
```

### View Results
```bash
maelstrom serve  # Opens http://localhost:8080
```

## Resources

- [Challenge Docs](https://fly.io/dist-sys/)
- [Maelstrom Repo](https://github.com/jepsen-io/maelstrom)
