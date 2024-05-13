import setuptools

setuptools.setup(
   name='boto_plus',
   version='1.0',
   description='',
   author='',
   author_email='',
   #packages=['boto_plus'],  #same as name
   packages=setuptools.find_packages(),
   install_requires=["setuptools >= 61.0", "pandas", "boto3"], #external packages as dependencies
)
