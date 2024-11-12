from setuptools import setup, find_packages
from os.path import join, dirname


setup(
    name='pipez',
    version='0.0.128',
    author="Alexander Timofeev",
    author_email="tam2511@mail.ru",
    python_requires=">=3.9",
    packages=find_packages(),
    long_description=open(join(dirname(__file__), 'README.md')).read(),
    long_description_content_type="text/markdown",
    url="https://github.com/tam2511/pipez",
    install_requires=[
        'numpy',
        'opencv-python',
        'fastapi',
        'uvicorn',
        'Jinja2'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: OS Independent",
    ],
    include_package_data=True
)
