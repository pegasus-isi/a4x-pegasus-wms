# Contributing

```sh
git clone https://github.com/pegasus-isi/a4x-pegasus-wms.git
```

## Tools

## pre-commit

```sh
cd a4x-pegasus-wms

# Install pre-commit
pip3 install -u pre-commit

# Install pre-commit hook
pre-commit install
```

## tox

```sh
pip3 install tox

# Unit tests
tox -e py37
```
