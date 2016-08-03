#!/bin/bash

venv=$(which virtualenv)
venv_name="$(basename $(git rev-parse --show-toplevel))_venv"
venv_path=$(git rev-parse --show-toplevel)/innovatively/${venv_name}

if [[ ! -d gumbo-parser ]]; then
    git clone https://github.com/google/gumbo-parser.git
    cd gumbo-parser
    ./autogen.sh
    ./configure --prefix=${venv_path}
    make
    make install
    python setup.py install

fi

source_lib=${venv_path}/lib/libgumbo.so
dest_lib=${venv_path}/local/lib/python2.7/site-packages/gumbo-0.10.1-py2.7.egg/gumbo/libgumbo.so
if [[ ! -e ${dest_lib} ]]; then
    cp ${source_lib} ${dest_lib}
fi
