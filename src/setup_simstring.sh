#!/bin/sh

set -e

usage()
{
cat << _USAGE_

Usage: $(basename $0) [--help] [--version SIMSTRING_VER] [--dir INSTALL_DIR] [--remove]

Install script for Simstring.
Simstring is downloaded from 'https://github.com/Georgetown-IR-Lab/simstring'

Requirements:
  Python3 (python3-dev)
  C compiler (gcc)
  wget
  tar

Default SIMSTRING_VER is '1.1.4'.
Default INSTALL_DIR is 'simstring'.
'remove' option removes a Simstring installation.

_USAGE_
}


#################
# CONFIGURATION #
#################
PYTHON_VERSION=python3
PROJECT_URL=https://github.com/Georgetown-IR-Lab/simstring
INSTALL_DIR=simstring
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
        -r | --remove)
            REMOVE_PROJECT=1
            shift ;;
         *) usage
            echo "ERROR! invalid command line option, $1"
            exit 1 ;;
    esac
done


################
# MAIN PROGRAM #
################
if [ $REMOVE_PROJECT -gt 0 ]; then
    echo "Removing Simstring ($INSTALL_DIR) ..."
    rm -rf "$INSTALL_DIR"
    exit 0
fi

RELEASE_FILENAME=$RELEASE_VERSION.tar.gz
RELEASE_URL=$PROJECT_URL/archive/$RELEASE_FILENAME
RELEASE_DIR=simstring-$RELEASE_VERSION

echo "Downloading Simstring ($RELEASE_URL) ..."
wget "$RELEASE_URL"

echo "Unpacking Simstring ($RELEASE_FILENAME) ..."
tar -xf "$RELEASE_FILENAME"
rm -f "$RELEASE_FILENAME"

echo "Building Simstring ($RELEASE_DIR/) ..."
cd "$RELEASE_DIR"
$PYTHON_VERSION setup.py build_ext --inplace
cd ..

echo "Installing ($INSTALL_DIR/) ..."
mkdir -p "$INSTALL_DIR"
touch "$INSTALL_DIR/__init__.py"
mv "$RELEASE_DIR/"_*.so "$INSTALL_DIR"
mv "$RELEASE_DIR/simstring.py" "$INSTALL_DIR/simstring.py"

rm -rf "$RELEASE_DIR"

echo "Simstring was installed successfully!"
