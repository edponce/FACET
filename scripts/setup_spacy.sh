#!/bin/sh

set -e

usage()
{
cat << _USAGE_

Usage: $(basename $0) [--help] [--corpus CORPUS]

Install script for spaCy's corpus.

Default CORPUS is 'en'.

Requirements:
    spacy

_USAGE_
}


#################
# CONFIGURATION #
#################
PYTHON_VERSION=python3
CORPUS=en


##########################
# COMMAND-LINE ARGUMENTS #
##########################
while [ "$1" ]; do
    case "$1" in
        -h | --help)
            usage
            exit 0 ;;
        -c | --corpus)
            CORPUS=$2
            shift 2 ;;
         *) usage
            echo "ERROR! invalid command line option, $1"
            exit 1 ;;
    esac
done


################
# MAIN PROGRAM #
################
$PYTHON_VERSION -m spacy download "$CORPUS"
