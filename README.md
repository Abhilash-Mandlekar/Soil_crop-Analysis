# Running the pipeline

Steps for windows:

1) clone the repository

2) Navigate to folder scripts

cd scripts

3) Change line number 7 in data_reader.py 

os.chdir should point to base folder.

4) Run command as follows on cmd (from scripts folder):

py transform.py "crop"
py transform.py "soil"
py transform.py "weather"
py transform.py "spectral"

The code currently only supports passing one argument.

5) Output will be stored in outputs folder.

