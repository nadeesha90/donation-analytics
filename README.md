# donation-analytics
insight data science challenge 

Implementation Details:
Transaction data is processed line by line from the input file using a python generator
If a transaction is valid (i.e. other_id = '' and other conditions) it is stored in a dictionary of lists indexed by the key: NAME + "_" + ZIP_CODE.
We also check whether the donor had contributed in a previous year and if so add this donor to a repeat_donors (python set).
Each transaction is also stored in separate dictionary of lists indexed by recepient, this is to allow fast look up of transactions
from a specific recepient.
If a transaction is from a repeat donor we do the required calculations and output to a file "repeat_donors.txt"
