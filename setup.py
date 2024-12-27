import os
import re

from setuptools import find_packages, setup


def get_version(package):
    """
    Return package version as listed in `__version__` in `__main__.py`.
    """
    path = os.path.join(package, "__main__.py")
    main_py = open(path, "r", encoding="utf8").read()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", main_py).group(1)


def get_long_description():
    """
    Return the README.
    """
    return open("README.md", "r", encoding="utf8").read()


def get_packages(package):
    """
    Return root package and all sub-packages.
    """
    return [
        dirpath
        for dirpath, dirnames, filenames in os.walk(package)
        if os.path.exists(os.path.join(dirpath, "__init__.py"))
    ]


setup(
    name="canvassyncer",
    version=get_version("canvassyncer"),
    url="https://github.com/BoYanZh/Canvas-Syncer",
    license="MIT",
    description="The async fast canavs file syncer.",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="SJTU JI Tech",
    author_email="bomingzh@sjtu.edu.cn",
    maintainer="BoYanZh",
    maintainer_email="bomingzh@sjtu.edu.cn",
    packages=find_packages(),
    python_requires=">=3.6",
    entry_points={
        "console_scripts": [
            "canvassyncer=canvassyncer:main",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/BoYanZh/Canvas-Syncer/issues",
        "Source": "https://github.com/BoYanZh/Canvas-Syncer",
    },
    install_requires=["httpx", "aiofiles", "tqdm"],
)
