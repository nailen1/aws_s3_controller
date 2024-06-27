from setuptools import setup, find_packages

setup(
    name='aws_s3_controller',
    version='0.1.3',
    packages=find_packages(),
    install_requires=[
        'boto3',
        'shining_pebbles',
    ],
    author='June Young Park',
    author_email='juneyoungpaak@gmail.com',
    description='Control S3. Manage, interact with, and handle S3 just like your local storage.',
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
