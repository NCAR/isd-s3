# Pyinstaller

In order to create a command line interface (cli) for your group that uses custom configuration, consider using `pyinstaller` to build a wrapper around the isd_s3 command line utility. This also is useful in that you can avoid problems with a poorly configured environment.


In our example (cli.py), we set up logging and our default environment from a .ini file. However, depending on your needs, you could initialize the script however you'd like. 

To create the binary file, we use the following command
```
pyinstaller cli.py --onefile
```
This will create a standalone executable with the correct environment. 
