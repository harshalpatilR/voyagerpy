import json
import csv
import pickle

##### GLOBAL
#### Define OUTPUT DICT
output_keys = \
["code","name","status","website","twitter","ind_cd","ind_name","esize_cd","esize_name","founded","desc","reach",
"locations","hq_ctry","hq_ctry_name","loc_1","loc_2","loc_3","loc_4","loc_5","loc_6","loc_7","loc_8","loc_9","loc_10",
"skills", "skill_1","skill_2","skill_3","skill_4","skill_5","skill_6","skill_7","skill_8","skill_9","skill_10",
"skill_11","skill_12","skill_13","skill_14","skill_15","skill_16","skill_17","skill_18","skill_19","skill_20","skill_21",
"skill_22","skill_23","skill_24","skill_25","Search_Keyword_All","Search_Keyword_Count"]
#"Keyword_1","Keyword_2","Keyword_3","Keyword_4","Keyword_5","Keyword_6","Keyword_7","Keyword_8","Keyword_9","Keyword_10",
#"Keyword_11","Keyword_12","Keyword_13","Keyword_14","Keyword_15","Keyword_16","Keyword_17","Keyword_18","Keyword_19","Keyword_20",
#"Keyword_21","Keyword_22","Keyword_23","Keyword_24","Keyword_25","Keyword_26","Keyword_27","Keyword_28","Keyword_29","Keyword_30",
#"Keyword_31","Keyword_32","Keyword_33","Keyword_34","Keyword_35","Keyword_36","Keyword_37","Keyword_38","Keyword_39","Keyword_40"]

output_keys_no_key_cols = \
["code","name","status","website","twitter","ind_cd","ind_name","esize_cd","esize_name","founded","desc","reach",
"locations","hq_ctry","hq_ctry_name","loc_1","loc_2","loc_3","loc_4","loc_5","loc_6","loc_7","loc_8","loc_9","loc_10",
"skills", "skill_1","skill_2","skill_3","skill_4","skill_5","skill_6","skill_7","skill_8","skill_9","skill_10",
"skill_11","skill_12","skill_13","skill_14","skill_15","skill_16","skill_17","skill_18","skill_19","skill_20","skill_21",
"skill_22","skill_23","skill_24","skill_25","Search_Keyword_All","Search_Keyword_Count"]


addr_keys =[]


print_result = False

################################################################
def print_x(arg):
        if print_result:
                print(arg)                
        return(1)


def print_set(result):
        global print_result
        print_result = result
        return(1)

################################################################
def process_variable_assignment(x):

        op_list = []
        valid = False
        
        y = x.partition("=")
        
        var = y[0].strip()
        equal = y[1].strip()
        value = y[2].strip()
        
        #### Zero bound
        if len(equal) > 0 and equal=="=":
                op_list.append(var)
                op_list.append(value)
                valid = True
                
        return(valid, op_list)

################################################################
def process_linkedin_json(company_code, output_list, dict, ki, act_keyword, ctry_map):
        #### company_code <- DICT, init to zero and then pass what gets returned
        #### dict <- json.loads page
        #### output_list <- Empty List First time, THEN whatever is returned
        #### Keyword index      


        ####Could be json.loads
        ####data = open("Page.JSON")

        ##### DICT -> PAGE
        #####dict = json.load(data)

        #### DICT -> Dict inside Page
        comp_list = dict["companies"]

        #### INT -> HOW MANY TOTAL Companies? IN ALL PAGES
        ###total_comp = comp_list["_total"]
        ###current_list_size = comp_list["_count"]

        #### LIST -> Individual Companies - This is LIST
        #### LIST IS ZERO BOUND
        indiv_comp_list = comp_list["values"]

        #### Global Definition
        global output_keys, output_keys_no_key_cols

        #### LIST of DICTs
        ####output_list = []

        #### Dict to check if unique code or duplicate
        ####company_code = {}
        ##### company_code KEY will be unique ID -> VALUE will be index into list of records

        ##### LOOP Parameters
        ji = 0
        jloopover = len(indiv_comp_list)
        #### Test
        ####jloopover = 1

        ######## Keyword number will be there
        ######ki = 0

        ######## 

        while ji < jloopover:
        
                #### OUTPUT DICT -> one row 
                output_dict = {}
                finit = 0
                flen = len(output_keys_no_key_cols)

                #### NOTE : initi without the keyword_xx columns
                while finit < flen:
                        output_dict[output_keys_no_key_cols[finit]] = ""
                        finit = finit+1

                ##### DICT -> EACH Company - This is DICT
                company = indiv_comp_list[ji]
        
                #### ALREADY exists - just increment values
                if company["id"] in company_code.keys():                                
                        #### RECORD KEYWORD --- 1 BASED         
                        idx = company_code[company["id"]]

                        #### REMOVED : vertical columns for keywords
                        ####output_list[idx]["Keyword_"+str(ki+1)] = str(1)

                        #### consolidated keywords & keyword counts - APPEND keywords                      
                        output_list[idx]["Search_Keyword_All"] = output_list[idx]["Search_Keyword_All"]+","+str(act_keyword)
                        output_list[idx]["Search_Keyword_Count"] = str(int(output_list[idx]["Search_Keyword_Count"])+1)

                ##### The company does not exist yet
                else:
                
                        #### PROCESS FIRST
                        if "id" in company.keys():
                                output_dict["code"] = str(company["id"])
                        if "name" in company.keys():
                                output_dict["name"] = company["name"]

                        if "status" in company.keys():                  
                                output_dict["status"] = company["status"]["name"]
                        if "websiteUrl" in company.keys():
                                output_dict["website"] = company["websiteUrl"]
                        if "twitterId" in company.keys():
                                output_dict["twitter"] = company["twitterId"]
                        if "industries" in company.keys():                      
                                if company["industries"]["_total"] > 0:
                                        output_dict["ind_cd"] = company["industries"]["values"][0]["code"]
                                        output_dict["ind_name"] = company["industries"]["values"][0]["name"]
                        
                        if "employeeCountRange" in company.keys():
                                output_dict["esize_cd"] = company["employeeCountRange"]["code"]
                                output_dict["esize_name"] = company["employeeCountRange"]["name"]
                                output_dict["esize_name"] = output_dict["esize_name"].replace("-","_")
                                
                        if "foundedYear" in company.keys():
                                output_dict["founded"] = str(company["foundedYear"])
                        if "description" in company.keys():
                                output_dict["desc"] = company["description"]
                        
                        if "numFollowers" in company.keys():
                                output_dict["reach"] = str(company["numFollowers"])

                        ##### PROCESS KEYWORD - THIS IS FIRST

                        #####output_dict["Keyword_"+str(ki+1)] = str(1)
                        output_dict["Search_Keyword_All"] = str(act_keyword)
                        output_dict["Search_Keyword_Count"] = str(1)

                        #### PROCESS SKILLS
                        if "specialties" in company.keys():
                                no_skills = company["specialties"]["_total"]
                        else:
                                no_skills = 0
                        
                        output_dict["skills"] = str(no_skills)
                        ls = 0
                
                        #### Max is 25
                        while ((ls < no_skills) and (ls < 25)):
                                if "specialties" in company.keys():
                                        output_dict["skill_"+str(ls+1)] = company["specialties"]["values"][ls]
                                ls = ls + 1


                        #### PROCESS LOCATIONS
                        if chk_key(company,"locations"):
                                no_locations = company["locations"]["_total"]
                                output_dict["locations"] = str(no_locations)
                                li = 0
                
                                #### Max is 10
                                while ((li < no_locations) and (li < 10)):
                                        if "city" in company["locations"]["values"][li]["address"].keys():
                                                output_dict["loc_"+str(li+1)] = company["locations"]["values"][li]["address"]["city"]

                                        #### 1st location is HQ
                                        if ("countryCode" in company["locations"]["values"][li]["address"].keys()) and (li == 0):
                                                ctry_code = company["locations"]["values"][li]["address"]["countryCode"]
                                                ctry_code = ctry_code.encode('ascii','ignore')
                                                output_dict["hq_ctry"] = ctry_code
                                                if chk_key(ctry_map,ctry_code):
                                                        output_dict["hq_ctry_name"] = ctry_map[ctry_code]

                                        li = li + 1


                        ###### ADD to OUTPUT LIST as ONE ROW
                        output_list.append(output_dict) 
                        company_code[company["id"]] = len(output_list) - 1
                

                #######
                ji = ji+1

        ##### ENCODE TO ASCII
        ai = 0

        while ai < len(output_list):
                keys = output_list[ai].keys()
                ki = 0
                while ki < len(keys):
                        ####print output_list[ai][keys[ki]]
                        output_list[ai][keys[ki]] = output_list[ai][keys[ki]].encode('ascii','ignore')
                        ki = ki+1

                ai = ai+1

        return(company_code, output_list)

################################################
def write_linkedin_output(output_list, fname, max_ki, poutput_dict_list=[]):
        global output_keys, addr_keys, output_keys_no_key_cols

        ### List is empty so use global dict definition
        if(len(poutput_dict_list) == 0):
                write_keys = output_keys_no_key_cols


        ### get set union and convert to list for all members and their keys()
        else:

                ## find longest dict keys list
                i = 0
                lloop = len(poutput_dict_list)

                while(i < lloop):
                        poutput_keys = poutput_dict_list[i].keys()
                        #### SET operations to combine lists
                        addr_keys = list(set(poutput_keys) | set(addr_keys))
                        i = i + 1

                #####Disabled as found of no use - Key match columns
                write_keys = addr_keys

        ######## WRITE CSV
        fcsv = open(fname,"wb")
        dw = csv.DictWriter(fcsv,write_keys)
        dw.writer.writerow(write_keys)
        dw.writerows(output_list)

        return(output_keys, output_list)

################################################
def process_linkedin_json_addr(company_code, output_list, pdict, ki, ctry_map):
        #### company_code <- DICT 
        #### dict <- json.loads page
        #### output_list <- Empty List First time, THEN whatever is returned
        #### Keyword index      
        ###### load json
        #### DICT -> Dict inside Page
        comp_list = pdict["companies"]
        
        #### LIST -> Individual Companies - This is LIST
        #### LIST IS ZERO BOUND
        indiv_comp_list = comp_list["values"]
        
        ##### LOOP Parameters
        ji = 0
        jloopover = len(indiv_comp_list)
        
        ######## 
        
        while ji < jloopover:
                
                ##### DICT -> EACH Company - This is DICT
                company = indiv_comp_list[ji]
                
                ##### The company does not exist yet
                if not company["id"] in company_code.keys():                            
                        #### RECORD KEYWORD --- 1 BASED         
                        ####idx = company_code[company["id"]]
                        ####output_list[idx]["Keyword_"+str(ki+1)] = str(1)

                        #### PROCESS LOCATIONS
                        if chk_key(company,"locations"):
                                no_locations = company["locations"]["_total"]

                                li = 0
                
                                #### Max is 10
                                while ((li < no_locations) and (li < 10)):
                                        #### OUTPUT DICT -> one row 
                                        output_dict = {}

                                        output_dict["locations"] = str(no_locations)
                                        output_dict["addr_seq_no"] = str(li+1)

                                        #### check KEY function to validate keys
                                        loc_keys = company["locations"]["values"][li]["address"].keys()
                                        loc_i = 0

                                        #### PROCESS FIRST
                                        if "id" in company.keys():
                                                output_dict["code"] = str(company["id"])
                                        if "name" in company.keys():
                                                output_dict["name"] = company["name"]

                                        #### ALL FIELDS OF ONE LOC - ADDRESS
                                        while loc_i < len(loc_keys):
                                                output_dict[loc_keys[loc_i]] = company["locations"]["values"][li]["address"][loc_keys[loc_i]]
                                                loc_i = loc_i + 1

                                                
                                        if ("countryCode" in company["locations"]["values"][li]["address"].keys()):
                                                ctry_code = company["locations"]["values"][li]["address"]["countryCode"]
                                                ctry_code = ctry_code.encode('ascii','ignore')
                                                if chk_key(ctry_map,ctry_code):
                                                        output_dict["country_name"] = ctry_map[ctry_code]


                                        #### ALL FIELDS OF ONE LOC - PHONE1, 2
                                        phone_keys = company["locations"]["values"][li]["contactInfo"].keys()        
                                        if "phone1" in phone_keys:
                                                output_dict["phone1"] = "'"+str(company["locations"]["values"][li]["contactInfo"]["phone1"].encode('ascii','ignore'))
                                        else:
                                                output_dict["phone1"] = "'"
                                        
                                        if "phone2" in phone_keys:
                                                output_dict["phone2"] = "'"+str(company["locations"]["values"][li]["contactInfo"]["phone2"].encode('ascii','ignore'))
                                        else:
                                                output_dict["phone2"] = "'"

                                        ##### ADD LOCATION
                                        ###### ADD to OUTPUT LIST as ONE ROW
                                        output_list.append(output_dict) 

                                                
                                        li = li + 1

                        #else:
                                        #### OUTPUT DICT -> one row 
                                        ###output_dict = {}
                                        ###output_dict["locations"] = 0
                                        
                                        #### PROCESS FIRST
                                        ##if "id" in company.keys():
                                        ##        output_dict["code"] = str(company["id"])
                                        #if "name" in company.keys():
                                        #        output_dict["name"] = company["name"]
                                                


                ji = ji + 1

        ##### ENCODE TO ASCII
        ai = 0

        while ai < len(output_list):
                keys = output_list[ai].keys()
                ki = 0
                while ki < len(keys):
                        ####print output_list[ai][keys[ki]]
                        
                        output_list[ai][keys[ki]] = output_list[ai][keys[ki]].encode('ascii','ignore')
                        ki = ki+1

                ai = ai+1

        return(company_code, output_list)

############## 
def chk_key(idict, ikey):
        if ikey in idict.keys():
                return(True)
        else:
                return(False)
        


############## KEYWORDS to global output_list
def keyword_fields(max_ki):
        global output_keys

        poutput_keys = output_keys
        ##### Keywords can be any number so keep it dynamic
        i=0
        while i< max_ki:
                poutput_keys.append("Keyword_"+str(i+1))
                i=i+1     

        return(1)



############## Pickle Linkedin country codes dictionary
def pickle_linkedin_ctry_codes(fname, oname):

        f = open(fname,"r")
        test = csv.DictReader(f, delimiter=',', quotechar='"')

        output_dict = {}

        for line in test:
            #print line.keys()
            #print line.values()
            output_dict[line['ID']] = line['Country Name']

        #print(output_dict)
        #print(output_dict['ch'])
        f.close()

        pickle.dump(output_dict, open(oname,'wb'))



################ READ pickled dict
def read_linkedin_ctry_codes(fname):

        output_dict1 = pickle.load(open(fname,'rb'))

        #for k in output_dict.keys():
        #    print k
        #    print output_dict1[k]
        #

        return output_dict1


############### UNIT TESTING
def main_test1():

        #### test config file processing
        fname = "voyager_config.txt"

        fh = open(fname,"r")
        lines = fh.readlines()
        fh.close()
        
        i=0
        while i<len(lines):        
                line_strip = lines[i].strip()
                line_strip = line_strip.upper()
                #### ignore blanks 
                if len(line_strip)>0:
                        first_char = line_strip[0]
                        ### ignore comments
                        if(not(first_char == "#")):
                                valid, op_list = process_variable_assignment(line_strip)
                                print(valid)
                                print(op_list)
                i = i+1
                
        
        ####Could be json.loads
        data = open("Page.JSON")
        
        ##### DICT -> PAGE
        pdict = json.load(data)

        ######
        company_code = {}
        output_list = []
        ki = 0

        company_code, output_list = process_linkedin_json_addr(company_code, output_list, pdict, ki)

        ######## WRITE CSV
        output_keys = output_list[0].keys()
        fcsv = open("XLS_addr.csv","wb")
        dw = csv.DictWriter(fcsv,output_keys)
        dw.writer.writerow(output_keys)
        dw.writerows(output_list)

        data.close()
        
        print(output_list)

if __name__ == '__main__':
        print("main")
        #####main()

        



