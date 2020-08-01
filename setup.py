import re
import configparser
import collections
from setuptools import setup, find_packages


with open('project.cfg') as fd:
    parser = configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation(),
    )
    parser.read_file(fd)
    project_info = dict(parser['project'])


def load_text(*filenames, delimiter='\n\n'):
    text = ''
    for i, fn in enumerate(filenames):
        try:
            with open(fn) as fd:
                text += fd.read()
        except Exception:
            pass
        else:
            if i > 0 and i + 1 < len(filenames):
                text += delimiter
    return text


def load_requirements(*filenames):
    requirements = []
    for fn in filenames:
        try:
            with open(fn) as fd:
                requirements += [line.strip() for line in fd.readlines()]
        except IOError:
            pass
    return requirements


def load_extras_requirements(*filenames):
    requirements = collections.defaultdict(list)
    for fn in filenames:
        try:
            with open(fn) as fd:
                for line in fd.readlines():
                    pkg = line.strip()
                    match = re.search(r'\[(.*)\]', pkg)
                    if match:
                        requirements[match.groups(1)[0]].append(
                            re.sub(r'\[.*\]', '', pkg)
                        )
        except IOError:
            pass
    return requirements


# For PyPI, the 'download_url' is a link to a hosted repository.
# Github hosting creates tarballs for download at
#   https://github.com/{username}/{project}/archive/{tag}.tar.gz.
# To create a git tag
#   git tag {name}-{version} -m 'Add project tag for PyPI'
#   git push --tags origin master
setup(
    name=project_info.get('name'),
    version=project_info.get('version'),
    description=project_info.get('description'),
    long_description=load_text('README.rst', 'LICENSE'),
    keywords=list(filter(
        None,
        map(str.strip, re.split(r',|\n', project_info.get('keywords', '')))
    )),
    url=project_info.get('url'),
    download_url=project_info.get('download_url'),
    author=project_info.get('author'),
    author_email=project_info.get('author_email'),
    license=project_info.get('license'),
    classifiers=[
        'Framework :: FACET',
        'Topic :: Documentation :: Sphinx',
        'Topic :: Software Development :: Libraries',
        'Topic :: Utilities',
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Operating System :: MacOS',
        'Operating System :: POSIX :: Linux',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Healthcare Industry',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
    ],
    platforms=['Linux'],
    zip_safe=False,
    python_requires='>=3.6,<=3.8',
    include_package_data=True,
    packages=find_packages(),
    install_requires=load_requirements('requirements.txt'),
    extras_require=load_extras_requirements('extras_requirements.txt'),
    entry_points={
        'console_scripts': [
            'facet=facet.scripts.cli:cli',
        ],
    },
)
