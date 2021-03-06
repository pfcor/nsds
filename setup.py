import setuptools

setuptools.setup(
    name="nsds",
    version="0.1.0",
    url="https://github.com/pfcor/nsds",

    author="Pedro Correia",
    author_email="pedro.correia@netshoes.com",

    description="Funções utilitárias do time de DS",
    long_description=open('README.rst').read(),

    packages=setuptools.find_packages(),

    install_requires=[],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python :: 3.6',
    ],
)
