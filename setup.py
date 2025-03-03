from os.path import dirname, join

from setuptools import find_packages, setup


setup(
    name='pipez',
    version='0.0.145',
    python_requires='>=3.9',
    install_requires=[
        'fastapi',
        'Jinja2',
        'numpy',
        'uvicorn'
    ],
    long_description=open(join(dirname(__file__), 'README.md')).read(),
    long_description_content_type='text/markdown',
    author='Alexander Timofeev',
    author_email='tam2511@mail.ru',
    url='https://github.com/tam2511/pipez',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)'
    ],
    license='GPLv3',
    include_package_data=True
)
