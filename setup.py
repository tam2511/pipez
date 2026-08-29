from setuptools import find_packages, setup


setup(
    name='pipez',
    version='0.0.160',
    python_requires='>=3.10',
    install_requires=[
        'fastapi',
        'Jinja2',
        'numpy',
        'pydantic',
        'uvicorn'
    ],
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
