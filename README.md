# Event Stream

This is a python package designed to facilitate event based communication through Redis Streams.

## How to Run

Running an **Event Stream** application is as simple as calling:

```bash
$ python ./src/event_stream/application.py example.json
```

This will launch two very basic busses, along with a master bus that may be used to shut everything down

## Build

```bash
$ make
```
Running `make` in this directory will build the default target "build". The chain of dependencies for the default target includes targets that build a docker image with development dependencies. This target will also run `pytest` and `build`, and copy the resulting `dist` directory with `.whl` and `.tar.gz` packages into the currect directory.

## Installation

After running build, you can install the default package by running

```
python -m pip install dist/my_package-0.1.0-py3-none-any.whl
```

...or, assuming you have an API token and a valid [`.pypirc` file](https://packaging.python.org/en/latest/specifications/pypirc/) you can upload and install your package from PyPI with

```bash
python3 -m twine upload dist/*
python3 -m pip install event_stream
```

See [Packaging Python Projects](https://packaging.python.org/en/latest/tutorials/packaging-projects/) for more details on uploading your package.

## Other targets

```bash
$ make setup
```
This target builds the docker image and is required by all other targets.

```bash
$ make run
```
This target runs the CLI application from a docker container indicated in `setup.cfg` with the arguments specified by `ARGUMENTS` in the `Makefile`. This target is mainly a convenience and reference for users. CLI arguments are more easily changed by running this underlying command directly from the command prompt.

```bash
$ make tests
```
This target runs `pytest` on the code in the image created by "setup" in a docker container.