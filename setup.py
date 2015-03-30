from distutils.core import setup

setup(
    name='python-mongomodel',
    version='0.1',
    author='Modesto Mas Tortosa',
    author_email='modesto@mtdev.co',
    packages=['mongomodel'],
    install_requires=[
        'git+https://github.com/mongodb/mongo-python-driver.git@3.0-dev',
        'python-dateutil'
    ],
    url='https://github.com/mastortosa/python-mongomodel.git',
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities'],
)
