from __future__ import print_function
from setuptools import setup, find_packages
import os
import shutil

here = os.path.abspath(os.path.dirname(__file__))

check_fpath = os.path.join("_swigvearch.so")
if not os.path.exists(check_fpath):
    print("Could not find {}".format(check_fpath))

# make the vearch python package dir
shutil.rmtree("vearch", ignore_errors=True)
os.mkdir("vearch")
shutil.copyfile("vearch.py", "vearch/__init__.py")
shutil.copyfile("swigvearch.py", "vearch/swigvearch.py")
shutil.copyfile("_swigvearch.so", "vearch/_swigvearch.so")

long_description="""
Vearch is the vector search infrastructure for deeping learning and AI applications. 
The Python's implementation allows vearch to be used locally.
"""
setup(
    name='vearch',
    version='0.3.0',
    description='A library for efficient similarity search and storage of deep learning vectors.',
    long_description=long_description,
    url='https://github.com/vearch/vearch',
    author='Haifeng Liu, Jie Li, Jian Sun, Mofei Zhang, Xingda Wang, Dabin Xie, Sen Gao, Jianyu Chen, Zhenyun Ni, Chao Zhan',
    author_email='vearch-maintainers@groups.io',
    license='Apache License, Version 2.0',
    keywords='real time index, vector nearest neighbors',

    install_requires=['numpy'],
    packages=['vearch'],
    package_data={
        'vearch': ['*.so'],
    },
    classifiers=[
        "Operating System :: POSIX :: Linux",
    ],
)
