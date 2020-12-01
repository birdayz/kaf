# Kaf
Kafka CLI inspired by kubectl & docker

[![Actions Status](https://github.com/birdayz/kaf/workflows/Go/badge.svg)](https://github.com/birdayz/kaf/actions)
[![GoReportCard](https://goreportcard.com/badge/github.com/birdayz/kaf)](https://goreportcard.com/report/github.com/birdayz/kaf)
[![GoDoc](https://godoc.org/github.com/birdayz/kaf?status.svg)](https://godoc.org/github.com/birdayz/kaf)

![asciicinema](asciicinema.gif)

## Install
Install from source:

```
go get -u github.com/birdayz/kaf/cmd/kaf
```

Install binary:

```
curl https://raw.githubusercontent.com/birdayz/kaf/master/godownloader.sh | BINDIR=$HOME/bin bash
```

Install on Archlinux via [AUR](https://aur.archlinux.org/packages/kaf/):

```
yay -S kaf
```

## Configuration
See the [examples](examples) folder

## Shell autocompletion
Source the completion script in your shell commands file:

Bash

```echo 'source <(kaf completion bash)' >> ~/.bashrc```

Zsh

```echo 'source <(kaf completion zsh)' >> ~/.zshrc```

Powershell

```Invoke-Expression (@(kaf completion powershell) -replace " ''\)$"," ' ')" -join "`n")```
