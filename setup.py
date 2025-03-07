from setuptools import setup, find_packages

setup(
    name='aws_s3_controller',
    version='0.7.5',
    packages=find_packages(),
    install_requires=[
        'boto3>=1.26.0',
        'python-dotenv>=1.0.0',
        'pandas>=1.3.0',
        'xlrd>=2.0.1',
        'shining_pebbles',
        'string_date_controller>=0.1.1'
    ],
    author='June Young Park',
    author_email='juneyoungpaak@gmail.com',
    description='A collection of natural language-like utility functions to intuitively and easily control AWS\'s cloud object storage resource, S3.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/nailen1/aws_s3_controller.git',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',
    keywords='aws s3 storage file-management data-processing',
)
