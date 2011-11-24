from distutils.core import setup

setup(
    name='pyres_scheduler',
    version='0.1',
    packages=['pyres_scheduler',],
    long_description=open('README.txt').read(),
    requires=[
        'APScheduler==2.0.2',
        'pyres==1.1',
        ]
)
