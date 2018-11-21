from setuptools import setup_cdn

setup(
    name='webotron-2018',
    version='0.1',
    author='Eduardo Ferreira',
    author_email='eduardo@eymea.com',
    Description='Webotron 2018 is a tool to deploy static websites to AWS.',
    license='GPLv3+',
    packages=['webotron'],
    url='https://github.com/eymea/py-automate-aws/tree/master/01-webotron',
    install_requires=[
        'click',
        'boto3'
    ],
    entry_points='''
        [console_scripts]
        webotron=webotron.webotron:cli
    '''
)
