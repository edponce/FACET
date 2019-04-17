#!/bin/sh

set -e

usage()
{
cat << _USAGE_

Usage: $(basename $0) [--help] [--version SIMSTRING_VER] [--dir INSTALL_DIR] [--tmp TMP_DIR] [--remove]

Install script for Simstring, overwrites existing installation.
Simstring is downloaded from 'https://github.com/Georgetown-IR-Lab/simstring'

Default SIMSTRING_VER is '1.1.4'.
Default INSTALL_DIR is 'simstring'. If INSTALL_DIR is not a fullpath, then it
  is relative to the current working directory.
Default TMP_DIR is '/tmp'.
The 'remove' option removes an existing Simstring installation and exits.

Requirements:
  Python3 (with python3-dev)
  C compiler (gcc)
  pwd
  curl | wget
  tar

_USAGE_
}


#################
# CONFIGURATION #
#################
PYTHON_VERSION=python3
PROJECT_URL=https://github.com/Georgetown-IR-Lab/simstring
INSTALL_DIR=simstring
TMP_DIR=/tmp
RELEASE_VERSION=1.1.4
REMOVE_PROJECT=0


##########################
# COMMAND-LINE ARGUMENTS #
##########################
while [ "$1" ]; do
    case "$1" in
        -h | --help)
            usage
            exit 0 ;;
        -v | --version)
            RELEASE_VERSION=$2
            shift 2 ;;
        -d | --dir)
            INSTALL_DIR=$2
            shift 2 ;;
        -t | --tmp)
            TMP_DIR=$2
            shift 2 ;;
        -r | --remove)
            REMOVE_PROJECT=1
            shift ;;
         *) usage
            echo "ERROR! invalid command line option, $1"
            exit 1 ;;
    esac
done


######################
# CHECK REQUIREMENTS #
######################
url_downloader=
if [ $(command -v curl) ]; then
    url_downloader="curl -O -L"
elif [ $(command -v wget) ]; then
    url_downloader="wget"
else
    echo "ERROR! failed to find a URL downloader application"
    exit 1;
fi


if [ $REMOVE_PROJECT -gt 0 ]; then
    echo "Removing Simstring ($INSTALL_DIR) ..."
    rm -rf "$INSTALL_DIR"
    exit 0
fi


################
# MAIN PROGRAM #
################
RELEASE_FILENAME=$RELEASE_VERSION.tar.gz
RELEASE_URL=$PROJECT_URL/archive/$RELEASE_FILENAME
RELEASE_DIR=simstring-$RELEASE_VERSION

# Where are we now?
CURR_DIR=$(pwd)

# Change to temporary directory
created_tmpdir=0
if [ ! -d "$TMP_DIR" ]; then
    mkdir "$TMP_DIR"
    created_tmpdir=1
fi
cd "$TMP_DIR"

echo "Downloading Simstring ($RELEASE_URL) ..."
$url_downloader "$RELEASE_URL"

echo "Unpacking Simstring ($RELEASE_FILENAME) ..."
tar -xf "$RELEASE_FILENAME"

echo "Building Simstring ($RELEASE_DIR/) ..."
cd "$RELEASE_DIR"
# NOTE: Redirect compiler warnings
$PYTHON_VERSION setup.py build_ext --inplace 2> /dev/null

# Change to original directory
cd "$CURR_DIR"

echo "Installing ($INSTALL_DIR/) ..."
mkdir -p "$INSTALL_DIR"
touch "$INSTALL_DIR/__init__.py"
mv "$TMP_DIR/$RELEASE_DIR/"_*.so "$INSTALL_DIR"
mv "$TMP_DIR/$RELEASE_DIR/simstring.py" "$INSTALL_DIR/simstring.py"

echo "Cleaning temporary files ($TMP_DIR/) ..."
if [ $created_tmpdir -eq 1 ]; then
    rm -rf "$TMP_DIR"
else
    rm -rf "$TMP_DIR/$RELEASE_DIR"
    rm -f "$TMP_DIR/$RELEASE_FILENAME"
fi

echo "Simstring was installed successfully!"
