# Testbed Prerequisites

All testbed variants require Git and a local clone of the repository:

```sh
git clone https://github.com/mumoshu/tapirs.git
cd tapirs
```

The table below shows which additional tools each variant needs. Click a tool name for installation instructions.

| Tool | [Docker Testbed](getting-started-testbed.md) | [Solo](getting-started-testbed-solo.md) | [K8s Testbed](getting-started-testbed-kube.md) | [K8s Operator](getting-started-testbed-kube-operator.md) |
|------|:-:|:-:|:-:|:-:|
| [Docker](https://docs.docker.com/get-docker/) | required | -- | required | required |
| [Rust toolchain](https://rustup.rs/) | -- | required | -- | -- |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | -- | -- | required | required |
| [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) | -- | -- | required | required |
| [Helm](https://helm.sh/docs/intro/install/) | -- | -- | -- | required |

## Rust toolchain (Solo testbed only)

The [Single-Node Mode](getting-started-testbed-solo.md) testbed builds tapirs from source. Install the Rust toolchain via [rustup](https://rustup.rs/) and build the binary:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install stable   # 1.93.0+
cargo build --release
```

Other testbed variants build inside Docker and do not require a local Rust installation.

## Verify your environment

Run the commands below to confirm each tool is available. You only need to verify the tools listed for your chosen variant.

```sh
git --version              # any recent version
docker --version           # 27+
rustc --version            # stable 1.93.0+; install via https://rustup.rs
kubectl version --client   # 1.35+
kind --version             # 0.31.0+
helm version               # 3.16+
```
