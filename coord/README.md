# DECODE Coordinator

This subdirectory contains the DECODE coordinator implementation written in Python with Flask.

## Getting Started

In the top-level DECODE repository, create a virtual envrionment and enter it:

    $ python3 -m venv .coord_venv
    $ source .coord_env/bin/activate(.fish|.csh)

This path is included in the gitignore, which prevents you from accidentally checking it into version control.

With the virtual envrionment activated, in the `coord` directory, run the following commands:

    (.coord_env) /coord/ $ pip3 install -r requirements.txt
    (.coord_env) /coord/ $ export PYTHONPATH=$(readlink -f .):$PYTHONPATH # for sh/bash/zsh/... shell
    (.coord_env) /coord/ $ set -gx PYTHONPATH (readlink -f .) $PYTHONPATH # for fish shell
    (.coord_env) /coord/ $ source .env
    (.coord_env) /coord/ $ flask db upgrade
    (.coord_env) /coord/ $ flask run
