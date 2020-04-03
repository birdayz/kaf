# Kaf
Kafka CLI inspired by kubectl & docker

[![CircleCI](https://circleci.com/gh/birdayz/kaf.svg?style=svg)](https://app.circleci.com/pipelines/github/birdayz/kaf)
[![GoReportCard](https://goreportcard.com/badge/github.com/birdayz/kaf)](https://goreportcard.com/report/github.com/birdayz/kaf)
[![GoDoc](https://godoc.org/github.com/birdayz/kaf?status.svg)](https://godoc.org/github.com/birdayz/kaf)

![asciicinema](asciicinema.gif)

## Install
Install from source:

```
go get github.com/birdayz/kaf/cmd/kaf
```

Install binary:

```
curl https://raw.githubusercontent.com/birdayz/kaf/master/godownloader.sh | BINDIR=$HOME/bin bash
```


## Configuration
See the [examples](examples) folder

## Shell autocompletion
Source the completion script in your shell commands file:

Bash

```echo 'source <(kaf completion bash)' >> ~/.bashrc```

Zsh

```echo 'source <(kaf completion zsh)' >> ~/.zshrc```
