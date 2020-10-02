# Contribution guide

Contributions to Gnomock and its ecosystem can be of any kind, but most of them
will probably be one of these:

- [Bug reports](#bug-reports)
- [Feature requests](#feature-requests)
- [Pull requests](#pull-requests)
    - [New presets](#new-presets)
- [Code review](#code-review)

## Bug reports

Gnomock like any other software has bugs, or behaves differently from what you
expect due to missing documentation. To report such cases, please open an issue
either in Gnomock main repository, or in the repository of an involved preset.

Please make sure to include steps to reproduce the issue you are facing. The
best way would be to link a repository that can be cloned. Any sufficient
code snippet inside the issue itself would also be fine.

## Feature requests

Gnomock should always move forward and add features that could make its users'
lives easier, and tests – better. If you think there is something that can help
the ecosystem, please submit a new issue either in Gnomock main repository, or
in the repository of an involved preset.

In the issue, please describe what would you like to see implemented, and why.
The best way would be by example: just write some code that uses the new
feature as if it existed, and describe what happens when the code runs.

If such an example is impossible to write, please try to explain your needs,
and we would try to come up with something together.

## Pull requests

If you decide to contribute actual code to one of Gnomock's repositories,
please let others know about it by opening a feature request you'd like to
implement, or a bug report you'd like to fix. This issue can be used to discuss
possible solutions. If you end up writing actual code, please make sure that
you follow existing conventions:

1. [Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)

2. Run [`golangci-lint`](https://github.com/golangci/golangci-lint) on your
   code using [.golangci.yml](.golangci.yml) file

3. Make sure all existing tests/examples pass, and add/modify tests to cover
   your changes

4. Document your changes: Go doc, example code in [README.md](README.md), pull
   request description, linked issue, etc.

5. Avoid breaking changes

### New presets

The obvious way to contribute to Gnomock is to implement a new Preset. Due to
an ambitious approach to development and distribution, this process goes beyond
writing the actual code that does the actual thing (implements a Preset). Below
are the steps that need to be taken care of in order to add a new Preset to
Gnomock:

> Please note that nobody has to go all the way alone. Adding a new Preset can
> be a collective effort, where some of the task are delegated to somebody else
> (to me, for example, I'm happy to help)

1. Write an actual preset code with a test. The code goes into
   [`preset`](https://github.com/orlangure/gnomock/tree/master/preset) package.
   You can use existing presets for reference, and are encouraged to do so.

1. Add the preset to the [preset
   registry](https://github.com/orlangure/gnomock/tree/master/preset/registry.go).
   This is the place used by
   [`gnomockd`](https://github.com/orlangure/gnomock/tree/master/gnomockd) to
   figure out what to do on incoming HTTP requests. This is required to allow
   projects in languages other than Go communicate with Gnomock container. Each
   preset in the registry has a test, which is different from the regular
   preset test: here the configuration comes from an HTTP request.

1. Update [swagger
   spec](https://github.com/orlangure/gnomock/blob/master/swagger/swagger.yaml)
   with the new endpoint. For example, for `memcached` preset the endpoint is
   `/preset/memcached`.

1. Generate [client SDK
   code](https://github.com/orlangure/gnomock#using-gnomock-server). Gnomock
   uses OpenAPI to generate client code from swagger specification.

1. Add client SDK tests for the new preset. These tests go into
   [`sdktest`](https://github.com/orlangure/gnomock/tree/master/sdktest)
   package. They allow to make sure that client code in languages other than Go
   does not break after each update.

1. Update [README](README.md) using the links to the new packages/docs.

## Code review

You are welcome to review existing pull requests, or existing code. Please be
civil, and try to back up your comments with whatever may help to convince the
other parties involved. For example, when pointing out a bug, present a way to
reproduce it (even if it is theoretical).
