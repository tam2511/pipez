from itertools import chain

from setuptools import setup, find_packages
from os.path import join, dirname

from pipez import __version__

requires = [

]

extra_requires = {

}

extra_requires["all"] = list(
    set(chain.from_iterable(extra_requires.values()))
)

setup(
    name='pipez',
    version=__version__,
    author="Alexander Timofeev",
    author_email="tam2511@mail.ru",
    python_requires=">=3.7",
    packages=find_packages(),
    long_description=open(join(dirname(__file__), 'README.md')).read(),
    long_description_content_type="text/markdown",
    url="https://github.com/tam2511/pipez",
    install_requires=requires,
    extras_require=extra_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: OS Independent",
    ],
)