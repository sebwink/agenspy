from setuptools import setup

setup(name='agenspy',
      version='0.0.1a1',
      url='https://github.com/sebwink/agenspy',
      license='BSD-3-Clause',
      author='Sebastian Winkler',
      author_email='sebwink@gmx.net',
      description='Package for working with AgensGraph',
      packages=['agenspy'],
      zip_safe=False,
      platform='any',
      python_requires='>=3.5',
      install_requires=[
          'psycopg2>=2.7'
      ],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: BSD License',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3 :: Only',
          'Topic :: Database',
          'Topic :: Database :: Front-Ends',
      ])
