from setuptools import setup, find_packages

setup(
    name='SMYTSyncDB',
    version='0.0.1',
    packages=find_packages(),
    include_package_data=True,
    url='www.fofpower.com',
    license='',
    author='Zhan JingHua',
    author_email='zhanjinghua@gmail.com',
    description='',
    install_requires=['pandas', 'sqlalchemy', 'pymysql', 'pyyaml'],
    entry_points={
        'console_scripts': [
            'smyt-sync-task = smyt_sync_manager.task:execute_from_commandline',
        ],
    },
    zip_safe=False,
    classifiers=[
        'Development Status :: Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Custom',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: FinTech :: Financial Database',
        'Topic :: Software Development :: Database :: ETL',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],

)
