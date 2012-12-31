#!/usr/bin/env python

from setuptools import setup, find_packages
from swiftnbd.const import version, project_url, description

def readme():
    try:
        return open("README.md").read()
    except:
        return ""

setup(name="swiftnbd",
      version=version,
      description=description,
      long_description=readme(),
      author="Juan J. Martinez",
      author_email="jjm@usebox.net",
      url=project_url,
      license="MIT",
      include_package_data=True,
      zip_safe=False,
      install_requires=["python-swiftclient>=1.2.0", "gevent>=0.13.8"],
      scripts=["bin/swiftnbdd", "bin/swiftnbd-setup"],
      packages=find_packages(exclude=["tests"]),
      classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Programming Language :: Python",
        "Operating System :: OS Independent",
        "Environment :: No Input/Output (Daemon)",
        "License :: OSI Approved :: MIT License",
        ],
      )
