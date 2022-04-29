# Kaf
Kafka CLI inspired by kubectl & docker

[![Actions Status](https://github.com/birdayz/kaf/workflows/Go/badge.svg)](https://github.com/birdayz/kaf/actions)
[![GoReportCard](https://goreportcard.com/badge/github.com/birdayz/kaf)](https://goreportcard.com/report/github.com/birdayz/kaf)
[![GoDoc](https://godoc.org/github.com/birdayz/kaf?status.svg)](https://godoc.org/github.com/birdayz/kaf)

![asciicinema](asciicinema.gif)

## Install
Install from source:

```
go install github.com/birdayz/kaf/cmd/kaf@latest
```

Install binary:

```
curl https://raw.githubusercontent.com/birdayz/kaf/master/godownloader.sh | BINDIR=$HOME/bin bash
```

Install on Archlinux via [AUR](https://aur.archlinux.org/packages/kaf/):

```
yay -S kaf-bin
```

## Usage

Show the tool version

`kaf --version`

Add a local Kafka with no auth

`kaf config add-cluster local -b localhost:9092`

Select cluster from dropdown list

`kaf config select-cluster`

Describe and List nodes

`kaf node ls`

List topics, partitions and replicas

`kaf topics`

Describe a given topic called _mqtt.messages.incoming_

`kaf topic describe mqtt.messages.incoming`

List consumer groups

`kaf groups`

Describe a given consumer group called _dispatcher_

`kaf group describe dispatcher`

Write message into given topic from stdin

`echo test | kaf produce mqtt.messages.incoming`

Set offset for consumer group _dispatcher_ consuming from topic _mqtt.messages.incoming_ to latest for all partitions

`kaf group commit dispatcher -t mqtt.messages.incoming --offset latest --all-partitions`

## Configuration
See the [examples](examples) folder

## Shell autocompletion
Source the completion script in your shell commands file:

Bash Linux:

```kaf completion bash > /etc/bash_completion.d/kaf```

Bash MacOS:

```kaf completion bash > /usr/local/etc/bash_completion.d/kaf```

Zsh

```kaf completion zsh > "${fpath[1]}/_kaf"```

Fish

```kaf completion fish > ~/.config/fish/completions/kaf.fish```

Powershell

```Invoke-Expression (@(kaf completion powershell) -replace " ''\)$"," ' ')" -join "`n")```
