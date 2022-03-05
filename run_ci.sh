#!/bin/sh

nosetests \
    --logging-level=INFO \
    --detailed-errors \
    --verbosity=2 \
    --with-coverage \
    --cover-package .
