from setuptools import setup, find_packages

subpackages = find_packages("src")
packages = ["dune_sync"] + ["dune_sync." + p for p in subpackages]

setup(
    name="dune_sync",
    version="1.6.4",
    packages=packages,
    package_dir={"dune_sync": "src"},
    include_package_data=True,
)
