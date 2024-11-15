import pathlib

from setuptools import setup

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()
with (HERE / "requirements.txt").open() as f:
    requirements = f.read().splitlines()

setup(
    name="marketdl",
    version="0.1.0",
    description="Reproducible market data downloader",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/asirenius/marketdl",
    author="asirenius",
    license="MIT",
    entry_points={
        "console_scripts": [
            "marketdl = marketdl.__main__:main",
        ],
    },
    python_requires=">=3.10",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    packages=["marketdl"],
    include_package_data=True,
    install_requires=requirements,
)
