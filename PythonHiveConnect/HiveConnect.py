from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd

import ConfigParser

Parser_file = "config.properties"

class Test_Data_validation(object):

     def __init__(self):
        self.cfg = ConfigParser.ConfigParser()
        self.cfg.read(Parser_file)
        self.query = self.cfg.get('TestQuery', 'query')
        self.get_hive_connection()
        self.cur = self.conn.cursor()
        self.patient_df = self.get_hive_data(self.query)
        #print self.patient_df
        self.pname =  self.patient_df['pname'].tolist()
        self.pdrug = self.patient_df['drug'].drop_duplicates().tolist()
        #print self.pname
        #print self.pdrug


        self.pname_flag = self.test_validate_pname(self.pname)

        self.pdrug_flag = self.test_validate_pdrug(self.pdrug)

        self.test_validate_pname = pd.DataFrame({'pname_len':[self.pname_flag], 'pdrug_validation':[self.pdrug_flag]})
        print  self.test_validate_pname



     def get_hive_connection(self):
        self.hostname = self.cfg.get('HostDetails', 'hostname')
        self.impalaport = self.cfg.get('HostDetails', 'port')
        self.conn = connect(host='192.168.0.129', port=self.impalaport)


     def get_hive_data(self, query):
        self.cur.execute(query)
        return as_pandas(self.cur)

     def test_validate_pname(self, pname ):
         pname_flag = True
         for name in pname:
             if ( len(name) > 30 ):
                 print "Name: ", name, "Len:   ", len(name)
                 pname_flag = False

         return "Pass" if pname_flag else "Fail"

     def test_validate_pdrug(self, pdrug):
         valid_drug_list = ['avil', 'paracetamol', 'metacin']
         pdrug_validation_flag = True
         pdrug_value_len = True

         for d1 in pdrug:
             if d1 not in valid_drug_list:
                 pdrug_validation_flag = False
                 print d1

         for drug in pdrug:
             if ( len(drug) > 12 ):
                 print "Drug: ", drug, "Len:   ", len(drug)
                 pdrug_value_len = False

         return "Pass" if ( pdrug_value_len  &  pdrug_validation_flag ) else  "Fail"

patient_details = Test_Data_validation()