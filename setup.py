import os
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

lib_fold = os.path.dirname(os.path.realpath(__file__))
requirements_path = lib_fold + "/requirements.txt"
install_requires = []
if os.path.isfile(requirements_path):
    with open(requirements_path) as f:
        install_requires = f.read().splitlines()

setuptools.setup(
    name="aquamonitor",
    version="0.3",
    author="Roar Brænden",
    author_email="roar.branden@niva.no",
    description="Python scripts to access Nivabasen",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NIVANorge/Aquamonitor-Python",
    py_modules=["aquamonitor"],
    packages=setuptools.find_packages(),
    install_requires=install_requires,
    python_requires=">=3.12",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU GPLv3",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
    ],
)
