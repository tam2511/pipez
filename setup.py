from os.path import dirname, join

from setuptools import find_packages, setup


setup(
    name='pipez',
    version='0.0.159',
    python_requires='>=3.10',
    install_requires=[
        'fastapi',
        'Jinja2',
        'numpy',
        'pydantic',
        'uvicorn'
    ],
    long_description=open(join(dirname(__file__), 'README.md')).read(),
    long_description_content_type='text/markdown',
    url='https://github.com/tam2511/pipez',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: 3.14',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)'
    ],
    license='GPLv3',
    include_package_data=True
)
