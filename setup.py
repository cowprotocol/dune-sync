from setuptools import setup, find_packages

subpackages = find_packages("src")
packages = ["src"] + ["src." + p for p in subpackages]

setup(
    name="src",
    version="1.6.4",
    packages=packages,
    package_dir={"dune_sync": "src/dune_sync"},
    include_package_data=True,
    install_requires=[
        "dune-client==1.7.4",
        "psycopg2-binary>=2.9.3",
        "python-dotenv>=0.20.0",
        "requests>=2.28.1",
        "pandas==2.1.4",
        "ndjson>=0.3.1",
        "py-multiformats-cid>=0.4.4",
        "boto3>=1.26.12",
        "SQLAlchemy<2.0",
    ],
)

