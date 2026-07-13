try:
    from setuptools import setup
    from setuptools import find_packages
except ImportError:
    from distutils.core import setup

try:
    with open("/accp/version") as f:
        __version__ = f.read().splitlines()[0]
except FileNotFoundError:
    # For non-ACCP environments, eg. local development
    __version__ = "0.1.1"


setup(name='rrap-gcp-dags',
      version=__version__,
      description='Rewrite Dags',
      author='RRAP',
      packages=['dags', 'functions', 'conf', 'bin'],
      include_package_data=True,
      author_email='nicholas.halam-andres@scotiabank.com')

