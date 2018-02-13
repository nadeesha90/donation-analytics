from datetime import datetime
import collections
import sys
import pdb
import math

class donation_analytics:
    def __init__(self,cont_file="itcont.txt",per_file="percentile.txt",ofile="repeat_donors.txt"):
        self.cont_file = cont_file
        self.percentile_file = per_file
        self.ofile = ofile
        self.percentile = self.getpercentile()
        self.col_ix_map = {
            'CMTE_ID':0,
            'NAME':7,
            'ZIP_CODE':10,
            'TRANSACTION_DT':13,
            'TRANSACTION_AMT':14,
            'OTHER_ID':15
            }

        #dict to hold contributions, key is name_zipcode    
        self.cont_db = collections.defaultdict(list)

        #dict storing contributions indexed by recepient 
        self.cont_db_recepient = collections.defaultdict(list)

        #set to hold repeat donors
        self.repeat_donors = set()

    #reads percentile file
    def getpercentile(self):
        with open(self.percentile_file,'r') as f:
            return int(f.read())

    #for each contribution yields dictionary
    def contributions_generator(self):
        def extract_contribution(line):
            tok = line.split('|')
            cont = {col:tok[self.col_ix_map[col]].strip() for col in self.col_ix_map}
            return cont

        with open(self.cont_file,'r') as f:
            for line in f:
                yield extract_contribution(line)

    #check if contribution is valid
    def is_valid_contribution(self,cont):
        #check other_id is empty
        if cont['OTHER_ID'] != '':
            #print "other_id not empty"
            return False

        #check recepient, transaction amount and donor name are not empty
        if len(cont['CMTE_ID'])==0 or len(cont['TRANSACTION_AMT'])==0 or len(cont['NAME'])==0:
            #print "recepient, amount or donor name is empty"
            return False
       
        #check zip code is long enough
        if len(cont['ZIP_CODE']) < 5:
            #print "zip code is invalid"
            return False

        #check is date is valid    
        if len(cont['TRANSACTION_DT']) == 8:
            mm = int(cont['TRANSACTION_DT'][:2])
            dd = int(cont['TRANSACTION_DT'][2:4])
            yy = int(cont['TRANSACTION_DT'][4:])
            if mm <= 12 and dd <= 31 and yy >= 0: 
                return True
            else:
                #print "invalid date"
                return False
        else:
            #print "invalid date"
            return False

        return True

    #insert contribution        
    def insert_contribution(self,cont):

        #transform contribution        
        def transform_contribution(cont):        
            cont['ZIP_CODE'] = cont['ZIP_CODE'][:5]
            cont['TRANSACTION_DT'] = datetime.strptime(cont['TRANSACTION_DT'],"%m%d%Y") 
            cont['TRANSACTION_AMT'] = float(cont['TRANSACTION_AMT'])
            return cont

        cont = transform_contribution(cont)

        key = cont['NAME'] + '_' + str(cont['ZIP_CODE'])
        cont_year = cont['TRANSACTION_DT'].year
        if key in self.cont_db:
            for c in self.cont_db[key]:
                if cont_year > c['TRANSACTION_DT'].year:
                    self.repeat_donors.add(key)            
                    break

        self.cont_db[key].append(cont)    
        self.cont_db_recepient[cont['CMTE_ID']].append(cont)
        return cont

    #check if repeat donor
    def is_repeat_donor(self,cont):
        key = cont['NAME'] + '_' + str(cont['ZIP_CODE'])
        return key in self.repeat_donors
   
    #perform running calculations
    def do_calculations(self,cont,percentile):
        #modify the storage datastructure and optimize it 
        def get_contributions(year,recepient,zip_code):
            conts = []
            for c in self.cont_db_recepient[recepient]:
                if c['TRANSACTION_DT'].year == year and c['ZIP_CODE'] == zip_code:
                    conts.append(c)
            return conts

        #total number of contributions
        def get_total_number(conts):
            return len(conts)

        #total amount of contributions
        def get_total_amount(conts):
            s = 0
            for c in conts:
                s += c['TRANSACTION_AMT']
            return s
    
        #calculate percentile of contribution
        def get_percentile(conts,percentile):
            ordinal_rank = int(math.ceil(float(percentile)/100.0*len(conts)))
            conts = sorted(conts,key=lambda contribution:contribution['TRANSACTION_AMT'])
            per = int(round(conts[ordinal_rank-1]['TRANSACTION_AMT']))
            return per

        year = cont['TRANSACTION_DT'].year
        recepient = cont['CMTE_ID']
        zip_code = cont['ZIP_CODE']
        
        contributions = get_contributions(year=year,recepient=recepient,zip_code=zip_code)
        number = get_total_number(contributions)
        amount = get_total_amount(contributions)
        percentile = get_percentile(contributions,percentile)

        return number,amount,percentile
        
    #generate output file
    def gen_output(self):
        with open(self.ofile,'w') as f:
            for contribution in self.contributions_generator():
                #print contribution
                if self.is_valid_contribution(contribution):
                    contribution = self.insert_contribution(contribution)
                    if self.is_repeat_donor(contribution):
                        number,amount,percentile = self.do_calculations(contribution,self.percentile)
                        cmte_id = contribution['CMTE_ID']
                        zip_code = contribution['ZIP_CODE']
                        year = contribution['TRANSACTION_DT'].year

                        line_str = '|'.join([cmte_id,zip_code,str(year),str(percentile),str(int(amount)),str(number)])
                        f.write(line_str + '\n')
                    

if __name__ == '__main__':
    input_file = sys.argv[1]
    percentile_file = sys.argv[2]
    output_file = sys.argv[3]

    da = donation_analytics(cont_file=input_file,per_file=percentile_file,ofile=output_file)
    da.gen_output()

