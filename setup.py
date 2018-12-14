"""
Snapdump setup
    Instructions:
    # Build:
    rm -rf dist/ snapdump.egg-info/ build
    python3 setup.py sdist bdist_wheel
    # Upload:
    python3 -m twine upload dist/*
"""
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    LONG_DESC = fh.read()
    setup(
        name="snapdump",
	scripts=['bin/snapdump'],
        version="1.0.3",
        author="Omry Yadan",
        author_email="omry@yadan.net",
        description="ZFS incremental snapshot dump and restore tool",
        long_description=LONG_DESC,
        long_description_content_type="text/markdown",
        url="https://github.com/omry/snapdump",
        keywords='zfs snapshot dump restore backup off-site',
	packages=find_packages(exclude=["config.yml"]),
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
        ],
        install_requires=['omegaconf>=1.0.7']
    )
