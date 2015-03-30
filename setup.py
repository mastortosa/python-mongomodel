from distutils.core import setup

setup(
    name='python-mongomodel',
    version='0.1',
    author='Modesto Mas Tortosa',
    author_email='modesto@mtdev.co',
    packages=['mongomodel'],
    install_requires=[
        'pymongo==3.0b0',
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
