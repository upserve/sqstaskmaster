import setuptools
from sqstaskmaster import version

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sqstaskmaster",
    version=version.__version__,
    author="Upserve",
    author_email="datascience@upserve.com",
    description="SQS Task Master for unwieldy python data processing by Upserve",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/upserve/sqstaskmaster",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["boto3", "botocore"],
    extras_require={"dev": ["callee", "flake8", "black", "cython"]},
    python_requires="~=3.7",
)
