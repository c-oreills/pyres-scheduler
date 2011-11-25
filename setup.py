from distutils.core import setup

setup(
    name='pyres-scheduler',
    version='0.1',
    author='Christy O\'Reilly',
    author_email='christy@oreills.co.uk',
    maintainer='Christy O\'Reilly',
    license='MIT',
    url='http://github.com/c-oreills/pyres-scheduler',
    packages=['pyres_scheduler',],
    long_description=open('README.txt').read(),
    requires=[
        'APScheduler (==2.0.2)',
        'pyres (==1.1)',
        ],
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python'
        ],
)
