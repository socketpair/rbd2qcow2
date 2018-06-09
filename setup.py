from setuptools import setup, find_packages

setup(
    name='rbd2qcow2',
    version='1.0.0',
    author='Коренберг Марк',
    author_email='socketpair@gmail.com',
    description='Ceph RBD incremental backup tool',
    long_description='Backup program which INCREMENTALLY back up CEPH RBD images to a chain of qcow2 files',
    # TODO: add license. https://pypi.org/pypi?%3Aaction=list_classifiers
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'Framework :: AsyncIO',
        'Intended Audience :: System Administrators',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: System :: Archiving :: Backup',
        'Topic :: System :: Clustering',
        'Topic :: Utilities',
    ],
    packages=find_packages(
        exclude=['tests', 'tests.*', '*.tests', '*.tests.*'],
    ),
    entry_points={
        'console_scripts': [
            'rbd2qcow2 = rbd2qcow2.main:main',
        ],
    },
    requires=[
        'rados',
        'rbd',
    ],
    test_suite='nose.collector',
)
