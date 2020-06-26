from setuptools import setup

setup(
    name='large-vcs',
    version='0.1.0',
    packages=['large_vcs'],
    license='MIT',
    author='Pranav Nutalapati',
    description='A version control system for large binary files.',
    install_requires=[
        'tqdm'
    ]
)
