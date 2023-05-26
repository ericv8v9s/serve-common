#!/bin/bash

python -m gunicorn "$@" 'example_app.entry:create_app()'
