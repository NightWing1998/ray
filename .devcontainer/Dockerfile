FROM mcr.microsoft.com/devcontainers/miniconda

RUN apt-get update && \
	apt-get install -y --no-install-recommends build-essential cmake ninja-build pkg-config gdb curl clang psmisc unzip && \
	rm -rf /var/lib/apt/lists/* && \
	curl -fsSL -o /usr/local/bin/bazel "https://github.com/bazelbuild/bazelisk/releases/download/v1.16.0/bazelisk-linux-$(dpkg --print-architecture)" && \
	chmod +x /usr/local/bin/bazel

USER vscode

RUN conda create -y --override-channels -c conda-forge -p /home/vscode/.conda/envs/raypy python=3.12 pip pytest
