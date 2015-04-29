#!/usr/bin/env python

from setuptools import setup, find_packages
from swiftnbd.const import version, project_url, description

def readme():
    try:
        return open("README.rst").read()
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
      install_requires=["python-swiftclient>=1.2.0", "gevent>=1.0"],
      scripts=["bin/swiftnbd-server", "bin/swiftnbd-ctl"],
      packages=find_packages(exclude=["tests"]),
      classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Programming Language :: Python :: 3 :: Only",
        "Operating System :: OS Independent",
        "Environment :: No Input/Output (Daemon)",
        "License :: OSI Approved :: MIT License",
        ],
      keywords="openstack object storage swift nbd",
      tests_require=["nose",],
      test_suite="nose.collector",
      )
