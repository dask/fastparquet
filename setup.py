"""setup.py - build script for parquet-python."""

import os
import sys
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext
import subprocess


class build_ext(_build_ext):
    # Kudos to https://stackoverflow.com/questions/19919905/how-to-bootstrap-numpy-installation-in-setup-py/21621689
    def finalize_options(self):
        import builtins
        _build_ext.finalize_options(self)
        # Prevent numpy from thinking it is still in its setup process:
        builtins.__NUMPY_SETUP__ = False
        import numpy
        self.include_dirs.append(numpy.get_include())


allowed = ('--help-commands', '--version', 'egg_info', 'clean')
if len(sys.argv) >= 2 and ('--help' in sys.argv[1:] or sys.argv[1] in allowed):
    # NumPy and cython are not required for these actions. They must succeed
    # so pip can install fastparquet when these requirements are not available.
    extra = {}
else:
    modules_to_build = {
        'fastparquet.speedups': ['fastparquet/speedups.pyx'],
        'fastparquet.cencoding': ['fastparquet/cencoding.pyx']
    }
    try:
        from Cython.Build import cythonize
        def fix_exts(sources):
            return sources
    except ImportError:
        def cythonize(modules, language_level):
            return modules
        def fix_exts(sources):
            return [s.replace('.pyx', '.c') for s in sources]

    modules = [
        Extension(mod, fix_exts(sources))
        for mod, sources in modules_to_build.items()]
    extra = {'ext_modules': cythonize(modules, language_level=3)}

install_requires = open('requirements.txt').read().strip().split('\n')
subprocess.call(["git", "status"], stdout=sys.stdout, stderr=sys.stderr)

setup(
    name='fastparquet',
    use_scm_version=True,
    description='Python support for Parquet file format',
    author='Martin Durant',
    author_email='mdurant@anaconda.com',
    url='https://github.com/dask/fastparquet/',
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    packages=['fastparquet'],
    cmdclass={'build_ext': build_ext},
    install_requires=install_requires,
    extras_require={
        'lzo': ['python-lzo'],
    },
    tests_require=[
        'pytest',
    ],
    long_description=(open('README.rst').read() if os.path.exists('README.rst')
                      else ''),
    include_package_data=True,
    exclude_package_data={'fastparquet': ['test/*']},
    python_requires=">=3.9",
    **extra
)
