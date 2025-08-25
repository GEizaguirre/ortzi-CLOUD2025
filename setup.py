import sys
import subprocess
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy


# Function to check if a command exists
def check_command(cmd):
    try:
        subprocess.run(
            [cmd, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


# Function to check required system packages
def check_system_dependencies():
    missing = []
    for cmd in ["gcc", "g++", "make", "zip"]:
        if not check_command(cmd):
            missing.append(cmd)

    if missing:
        print("\nERROR: Missing system dependencies:", ", ".join(missing))
        print("Please install them before proceeding.")
        if sys.platform.startswith("linux"):
            print(
                "For Debian/Ubuntu, run: sudo apt update &&",
                "sudo apt install build-essential zip"
            )
        else:
            print("Systems other than Linux are not supported.")
        sys.exit(1)


# Check dependencies before running setup
check_system_dependencies()

# Cython Extension
ext_modules = [
    Extension(
        "ortzi.io_utils.read_terasort_data",
        ["ortzi/cython/read_terasort_data.pyx"],
        include_dirs=[numpy.get_include()]
    )
]

setup(
    ext_modules=cythonize(ext_modules),
)
