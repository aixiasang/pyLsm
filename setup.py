"""
Setup script for PyLSM package
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = f.read().splitlines()

setup(
    name="pylsm",
    version="0.1.0",
    author="PyLSM Team",
    author_email="example@example.com",
    description="A lightweight LSM tree based key-value storage engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/pylsm",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
) 