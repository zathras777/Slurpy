import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "slurpy",
    version = "0.0.1",
    author = "david reid",
    author_email = "zathrasorama@gmail.com",
    description = ("A project to backup Lightroom Catalogs to postgresql."),
    license = "BSD",
    keywords = "database lightroom postgresql backup catalog",
    url = "http://www.david-reid.com/projects/slurpy.html",
    packages=['slurpy'],
    long_description=read('README'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
    entry_points="""
    [console_scripts]
    slurpy = slurpy.lrCatalog:main
    """,
    test_suite='slurpy.tests'
)
