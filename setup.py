from setuptools import setup, find_packages

setup(
    name="apidag",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "networkx>=2.0",
        "aiohttp>=3.8.0",
        "jsonpath-ng>=1.5.0"
    ],
)