# WeatherXM QoD

## Build
The following script creates a python environment using `poetry` and `pyenv` (need to be installed beforehand).
For specific dependency groups check the Makefile.
```bash
    brew install pyenv

    pyenv install 3.10.12

    pyenv local 3.10.12

    pip install poetry

    make install-all
```

The default venv installation path can be changed using:
```bash
    poetry config virtualenvs.path
```

## Environment

Copy the `.env.template` file to a new `.env` file.


## IDE Settings

### Intellij import fix
Simply navigate to Project Settings -> Modules and then set 'Source Root' the top level 'src' folder.

### Plugins
* ruff (Intellij)
