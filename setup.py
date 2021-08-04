import pathlib
from setuptools import setup

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name="scheduguru",
    version="0.0.5",
    description="Schedule Python functions with ease",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/doublevcodes/scheduguru",
    author="Vivaan Verma",
    author_email="vivaan.verma@gmail.com",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ],
    packages=["scheduguru"],
    include_package_data=True,
    install_requires=["loguru"]
)
