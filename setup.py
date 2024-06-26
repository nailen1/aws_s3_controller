from setuptools import setup, find_packages

setup(
    name='aws_s3_controller',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'boto3',
        'shining_pebbles',
    ],    
    author='June Young Park',
    author_email='juneyoungpaak@gmail.com',
    description='A collection of utility functions that enable treating a file system of multiple files as a pseudo-database, facilitating maintenance and operations across the large-scale file system. My shining pebbles.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/nailen1/aws_s3_controller.git',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
