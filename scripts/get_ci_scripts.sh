#!/bin/bash

REPO="git@github.com:librato/ci-scripts.git"
BASEDIR="$PWD"

git clone -q $REPO ci-scripts || exit 1

