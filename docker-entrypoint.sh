#!/bin/bash
# Trigger an error if non-zero exit code is encountered
set -e

case "$1" in
    "default")
        exec python -m scripts.main
    ;;
    *)
        exec ${@}
    ;;
esac