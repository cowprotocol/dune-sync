import os
from setuptools import setup, find_packages

subpackages = find_packages("src")
packages = ["src"] + ["src." + p for p in subpackages]


def read_requirements(filename):
    with open(filename, "r") as f:
        return [line.strip() for line in f.readlines() if line.strip()]


setup(
    name="src",
    version="1.6.4",
    packages=packages,
    package_dir={"dune_sync": "src/dune_sync"},
    include_package_data=True,
    data_files=[
        (
            os.path.join(
                "lib", "python{0}.{1}".format(*os.sys.version_info[:2]), "site-packages"
            ),
            ["logging.conf"],
        )
    ],
    install_requires=read_requirements("requirements/prod.txt"),
)
