# Supported target: linux/amd64. Pins resolved from official sources 2026-11-16.
FROM rust:1.99.1-bookworm@sha256:4715c3ad743951a3cea90e8b432f2a17e0ef5f3f3c3abf75108879e783b396f0 AS build
RUN useradd --uid 10001 --create-home swarm \
    && install -d -o 10001 -g 10001 /src /target
WORKDIR /src
ENV CARGO_HOME=/home/swarm/.cargo CARGO_TARGET_DIR=/target RUSTUP_TOOLCHAIN=1.99.1 \
    CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
COPY --chown=10001:10001 . .
USER 10001:10001
# Exercise the Linux platform as the same unprivileged UID as the runtime.
RUN cargo build --locked -p eliot -p eliot-daemon -p eliot-plugin-opencode --all-targets \
    && cargo test --locked -p eliot-daemon --lib platform:: \
    && cargo test --locked -p eliot-plugin-opencode --lib native::client::tests \
    && cargo test --locked -p eliot --bin swarm

FROM debian:bookworm-slim@sha256:c67e92c8099ba22853ab4cfda7cfb494d253d6012ca82dee98ba8976772af018 AS runtime-base
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash ca-certificates curl git jq libatomic1 procps ripgrep unzip xz-utils \
    && rm -rf /var/lib/apt/lists/*

FROM runtime-base AS harnesses
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
# Official versioned installers. Keep their installed packages and notices together.
RUN curl -fsSL https://chatgpt.com/codex/install.sh -o /tmp/codex-install.sh \
    && CODEX_HOME=/opt/codex CODEX_INSTALL_DIR=/usr/local/bin \
       CODEX_NON_INTERACTIVE=true sh /tmp/codex-install.sh --release 0.167.1 \
    && /usr/local/bin/codex --version
RUN mkdir -p /opt/claude \
    && curl -fsSL https://claude.ai/install.sh -o /tmp/claude-install.sh \
    && HOME=/opt/claude bash /tmp/claude-install.sh 2.1.325 \
    && ln -s /opt/claude/.local/bin/claude /usr/local/bin/claude \
    && claude --version
RUN mkdir -p /opt/opencode \
    && curl -fsSL https://opencode.ai/v2/install -o /tmp/opencode-install.sh \
    && HOME=/opt/opencode bash /tmp/opencode-install.sh --version 2.2.7 --no-modify-path \
    && ln -s /opt/opencode/.opencode/bin/opencode /usr/local/bin/opencode \
    && opencode --version
# Official release artifacts selected by the vendors' Linux installer manifests.
# Muse's launcher auto-updates, so invoke its frozen native payload directly.
RUN curl -fsSL 'https://lookaside.facebook.com/lookaside/muse/download/?channel=muse&version=1.4.0-R4161.1&file=muse-x86-linux' -o /usr/local/bin/muse \
    && echo '1b68bd4518d53a2aaff063915df4d141b0a205e6d79038299d04e3a14e85a5b9  /usr/local/bin/muse' | sha256sum --check \
    && chmod 0755 /usr/local/bin/muse \
    && muse --version
RUN curl -fsSL 'https://storage.googleapis.com/antigravity-public/antigravity-cli/1.5.9-4515810895593472/linux-x64/cli_linux_x64.tar.gz' -o /tmp/agy.tar.gz \
    && echo 'd01eb13ba7750eed18907f1ae8860864371e0ad9064415e8d033d22b4bd7b9b4e4c7f962058520b452d67db1c33163d9a889d55a7dffa159596ec6cd91a87b79  /tmp/agy.tar.gz' | sha512sum --check \
    && mkdir /opt/agy \
    && tar -xzf /tmp/agy.tar.gz -C /opt/agy \
    && ln -s /opt/agy/antigravity /usr/local/bin/agy \
    && agy --version

FROM runtime-base AS runtime
COPY --from=harnesses /opt/ /opt/
COPY --from=harnesses /usr/local/bin/ /usr/local/bin/
COPY --from=build /target/debug/swarm /usr/local/bin/swarm
RUN useradd --uid 10001 --create-home swarm \
    && install -d -m 0700 -o 10001 -g 10001 /data /workspace \
       /home/swarm/.codex /home/swarm/.claude /home/swarm/.local \
       /home/swarm/.local/share /home/swarm/.local/share/opencode
ENV HOME=/home/swarm ELIOT_HOME=/data DISABLE_AUTOUPDATER=1
WORKDIR /workspace
VOLUME ["/data"]
USER 10001:10001
STOPSIGNAL SIGINT
ENTRYPOINT ["swarm"]
CMD ["daemon", "--foreground"]
