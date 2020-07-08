import os
from setuptools import setup, find_packages
import meta


def get_text_from_files(*filenames, delimiter=os.linesep * 2):
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


def get_requirements_from_files(*filenames):
    requirements = []
    for fn in filenames:
        try:
            with open(fn) as fd:
                requirements += [line.strip() for line in fd.readlines()]
        except Exception:
            pass
    return requirements


long_description = get_text_from_files('README.rst', 'LICENSE')
install_requirements = get_requirements_from_files('requirements.txt')
extras_requirements = {
    'reST': ['Sphinx>=3.0', 'sphinx_rtd_theme>=0.4.3', 'sphinx-click>=2.3'],
    'lint': ['flake8>=3.5'],
    'coverage': ['coverage>=4.5'],
    'test': ['pytest>=5.3'],
}


# For PyPI, the 'download_url' is a link to a hosted repository.
# Github hosting creates tarballs for download at
#   https://github.com/{username}/{package}/archive/{tag}.tar.gz.
# To create a git tag
#   git tag meta.__name__-meta.__version__ -m 'Adds a tag so that we can put
#                                            package on PyPI'
#   git push --tags origin master
setup(
    name=meta.__name__,
    version=meta.__version__,
    description=meta.__description__,
    long_description=long_description,
    keywords=meta.__keywords__,
    url=meta.__url__,
    download_url=(
        f'{meta.__url__}/archive/{meta.__name__}-{meta.__version__}.tar.gz'
    ),
    author=meta.__author__,
    author_email=meta.__author_email__,
    license=meta.__license__,
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
    install_requires=install_requirements,
    extras_require=extras_requirements,
    entry_points={
        'console_scripts': [
            'facet=facet.scripts.facet:main',
        ],
    },
)
