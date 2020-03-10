import re
import QuickerUMLS as pkg
from setuptools import setup, find_packages


# Load long description from files
long_description = ""
try:
    with open("README.rst") as fd:
        long_description = fd.read()
except Exception:
    pass

# A list of strings specifying what other distributions need to be installed
# when this package is installed.
install_requirements = []
try:
    with open("install_requirements.txt") as fd:
        install_requirements = [l.strip() for l in fd.readlines()]
except Exception:
    pass

# A list of strings specifying what other distributions need to be present
# in order for this setup script to run.
setup_requirements = []
try:
    with open("setup_requirements.txt") as fd:
        setup_requirements = [l.strip() for l in fd.readlines()]
except Exception:
    pass

# A list of strings specifying what other distributions need to be present
# for this package tests to run.
tests_requirements = []
try:
    with open("tests_requirements.txt") as fd:
        tests_requirements = [l.strip() for l in fd.readlines()]
except Exception:
    pass

# A dictionary mapping of names of "extra" features to lists of strings
# describing those features' requirements. These requirements will not be
# automatically installed unless another package depends on them.
extras_requirements = {}
try:
    regex = re.compile(r"^(.+[<>=]+\d+[\.?\d*]*)\s*\[(.+)\][\r\n]")
    with open("extras_requirements.txt") as fd:
        for line in fd:
            match = regex.fullmatch(line)
            if match:
                value, key = match.group(1, 2)
                if key in extras_requirements:
                    extras_requirements[key].append(value)
                else:
                    extras_requirements[key] = [value]
except Exception:
    pass

# For PyPI, the 'download_url' is a link to a hosted repository.
# Github hosting creates tarballs for download at
#   https://github.com/{username}/{package}/archive/{tag}.tar.gz.
# To create a git tag
#   git tag pkg.__name__-pkg.__version__ -m 'Adds a tag so that we can put
#                                            package on PyPI'
#   git push --tags origin master
setup(
    name=pkg.__name__,
    version=pkg.__version__,
    description=pkg.__description__,
    long_description=long_description,
    keywords=pkg.__keywords__,
    url=pkg.__url__,
    download_url="{}/archive/{}-{}.tar.gz".format(pkg.__url__,
                                                  pkg.__name__,
                                                  pkg.__version__),
    author=pkg.__author__,
    author_email=pkg.__author_email__,
    license=pkg.__license__,
    classifiers=[
        "Development Status :: 1 - Planning",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Topic :: Documentation :: Sphinx",
        "Topic :: Utilities",
        "Topic :: Software Development :: Libraries",
    ],
    platforms=["Linux"],
    zip_safe=False,
    python_requires=">=3.7",
    include_package_data=True,
    # packages=find_packages(),
    packages=[
        'QuickerUMLS',
        'QuickerUMLS.database',
        'QuickerUMLS.serializer',
        'QuickerUMLS.tokenizer',
        'QuickerUMLS.web',
        'QuickerUMLS.simstring',
        'QuickerUMLS.simstring.similarity',
    ],
    install_requires=install_requirements,
    setup_requires=setup_requirements,
    extras_require=extras_requirements,
    tests_require=tests_requirements,
    test_suite="tests",
)
