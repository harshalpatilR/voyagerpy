##### WRITES FIRST PAGE SO WE GET COUNTS SPECIFIC TO COUNTRY+KEYWORD

import oauth2 as oauth
import httplib2
import time, os, simplejson
import time
import json
import csv
import X150206_Python_JSON_Commands as pj
import os.path
import sys


################################# 
# Fill the keys and secrets you retrieved after registering your app
#consumer_key      =   '75xctudidpev89'
#consumer_secret  =   'PoSFnjBukXTD3KAS'
#user_token           =   'b058dff8-1b4c-4ec4-80ac-8f278c6603b8'
#user_secret          =   '6baeb433-be1c-4b77-af03-8e83eff83b3d'

#consumer_key      =   'x'
#consumer_secret  =   'x'
#user_token           =   'x'
#user_secret          =   'x'



################################## SPECIFY DEFAULTS
###### FAST MODE
fast_mode = False

##### GENERATE COUNTS ONLY?
gen_counts = True

##### CTRY
ctry = "au:0"

####,kr_0"
####ctry = "in"

##### LOAD keywords
f_name = "Keywords.txt"

##### print details
print_details = False

###### Keyword_offset
###### ONLY -1 for WIN
###### USE .strip to remove unwanted space and /n at end
Keyword_offset = 0


####### Mode for extract
IND_MODE = 2
KEY_MODE = 1
mode = KEY_MODE

######path for raw file storage
raw_path = "./raw files/"
file_prefix = "Project_OP_"

####### Max level of employees
empl_code = "C"


################################ disable print if not req
pj.print_set(print_details)

################################# READ CONFIG FILE
#### test config file processing
fname = "voyager_config.txt"

fh = open(fname,"r")
lines = fh.readlines()
fh.close()

#####print lines

i=0
while i<len(lines):        
        line_strip = lines[i].strip()
##        line_strip = line_strip.upper()
        
        #### ignore blanks 
        if len(line_strip)>0:
                first_char = line_strip[0]
                ### ignore comments
                if(not(first_char == "#")):
                        valid, op_list = pj.process_variable_assignment(line_strip)
                        #####print valid, op_list

                        if(valid):
                                if(op_list[0] == "FILE"):
                                        f_name = op_list[1]
                                        pj.print_x("--FILE")
                                        pj.print_x(op_list[1])

                                elif(op_list[0] == "CTRY"):
                                        pj.print_x("--CTRY")
                                        pj.print_x(op_list[1])
                                        ctry = op_list[1].lower()

                                elif(op_list[0] == "COUNTS"):
                                        pj.print_x("--COUNTS")
                                        pj.print_x((op_list[1]=="TRUE"))
                                        gen_counts = (op_list[1]=="TRUE")
                                        
                                elif(op_list[0] == "FAST"):
                                        pj.print_x("--FAST")
                                        pj.print_x((op_list[1]=="TRUE"))
                                        fast_mode = (op_list[1]=="TRUE")

                                elif(op_list[0] == "PRINT_DETAILS"):
                                        pj.print_x("--PRINT_DETAILS")
                                        pj.print_x((op_list[1]=="TRUE"))
                                        print_details = (op_list[1]=="TRUE")
                                        pj.print_set(print_details)

                                elif(op_list[0] == "MODE"):
                                        pj.print_x("--MODE")
                                        if(op_list[1]=="INDUSTRY"):
                                                mode = IND_MODE
                                        else:
                                                mode = KEY_MODE

                                        pj.print_x(mode)

                                elif(op_list[0] == "EMPL"):
                                        pj.print_x("--EMPL_CODE")
                                        ###if(op_list[1] in ("A","B","C","D","E","F","G","H","I")):
                                        empl_code = (op_list[1].strip()).upper()
                                        pj.print_x(empl_code)

                                elif(op_list[0] == "IND_CODE"):
                                        pj.print_x("--IND_CODE")
                                        ind_code = (op_list[1].strip()).upper()
                                        pj.print_x(ind_code)

                                elif(op_list[0] == "CONSUMER_KEY"):
                                        pj.print_x("--CONSUMER_KEY")
                                        consumer_key = str(op_list[1].strip())

                                elif(op_list[0] == "CONSUMER_SECRET"):
                                        pj.print_x("--CONSUMER_SECRET")
                                        consumer_secret = str(op_list[1].strip())

                                elif(op_list[0] == "USER_TOKEN"):
                                        pj.print_x("--USER_TOKEN")
                                        user_token = str(op_list[1].strip())

                                elif(op_list[0] == "USER_SECRET"):
                                        pj.print_x("--USER_SECRET")
                                        user_secret = str(op_list[1].strip())


                                elif(op_list[0] == "RAW_FILES_PATH"):
                                        pj.print_x("--RAW_FILES_PATH")
                                        raw_path = str(op_list[1].strip())
                                        print raw_path

                                elif(op_list[0] == "FILE_PREFIX"):
                                        pj.print_x("--FILE_PREFIX")
                                        file_prefix = str(op_list[1].strip())
                                        print file_prefix

                                else:
                                        print "CONFIG FILE : Unknown Command : %s = %s" %(op_list[0],op_list[1])
                                        sys.exit()        


        i = i+1

#print consumer_secret
#print user_secret

################################ disable print if not req - AFTER CONFIG
pj.print_set(print_details)
#####,kr:0
win_file_ctry = ctry.replace(":","_")


 
# Use your API key and secret to instantiate consumer object
consumer = oauth.Consumer(consumer_key, consumer_secret)
 
# Use the consumer object to initialize the client object
client = oauth.Client(consumer)
 
# Use your developer token and secret to instantiate access token object
access_token = oauth.Token(
            key=user_token,
            secret=user_secret)
 
client = oauth.Client(consumer, access_token)

#### FAST MODE
if fast_mode:
        print("-------Fast mode is ON")

##### GENERATE COUNTS ONLY?
gen_counts_dict = {}

if gen_counts:
        genf = open("KEY_COUNTS.txt","w")
        print("--------COUNTS mode is ON")

###### --------- COMPANY SEARCH API
# loop will not execute at all if not INIT properly
x = 0

##### MANUAL fill from output either website or company count script
no_in_collection = 20

### USER MODIFIED ****************
###### -----------------------------------
n_file = open(f_name, "r")
keywords = n_file.readlines()
orig_keywords = []

#print("KEYWORDS --------------------------------------------")
#print(keywords)
#print len(keywords)
n_file.close()

##### REPLACE space with %20
##### PYTHON HAS ZERO BASED ARRAYS
i = 0
#####keyword_len = len(keywords)


###### MAKE directory before pull
if not (os.path.exists(raw_path)):
        os.makedirs(raw_path)
        

###### "Keywords" variable can include either a real keyword OR industry code depending upon mode
###### STRIP removes ending /n from string
while i< len(keywords):

        orig_keywords.append(keywords[i].strip())
        keywords[i] = keywords[i].strip()
        keywords[i] = keywords[i].replace(" ","%20")
        if Keyword_offset <> 0:
                keywords[i] = keywords[i][:Keyword_offset]
        else:
                keywords[i] = keywords[i]
        i = i+1

pj.print_x("KEYWORDS : REPLACED space ---------------------------")
pj.print_x(keywords)
pj.print_x("-----------------------------------------------------")
pj.print_x("ORIGINAL --------------------------------------------")
pj.print_x(orig_keywords)


######
###### Adjust keywords in field list
pj.keyword_fields(len(keywords))

##### EMPTY DICTS
company_code = {}
acompany_code = {}
output_list = []
aoutput_list = []

###### Keyword Loop
ki=0
loopkeywords = len(keywords)
##### TEST
#####loopkeywords = 1

###### Read pickled country map
ctry_map = pj.read_linkedin_ctry_codes('Linkedin_ctry_codes.dict')

###### EACH KEYWORD LOOP
while ki< loopkeywords:
        pj.print_x("---------- KEYWORD --------")
        pj.print_x(keywords[ki])

        #### Next loop should trigger
        x = 0
        no_in_collection = 200

        ####### Search Loop

        #### EACH SEARCH LOOP - go through ALL pages
        #### EXCL logo-url,stock-exchange,end-year, locations,
        while x < no_in_collection:
        #####   while x < 40:

                        #### make sure employees are used only if not all
                        if not (empl_code == "ALL"):
                                ### EMPL facility
                                reqe = "&facet=company-size," + empl_code
                        else:
                                reqe = ""

                        #### pick keywords only in keyword more - NOT needed in industry extraction
                        if (mode == KEY_MODE):
                                reqkeyw = "&keywords={" + str(keywords[ki]) + "}"
                        else:
                                reqkeyw = "&facet=company-size," + str(keywords[ki])

                        #### Pagination
                        reqend = "&sort=relevance&count=20&start=" + str(x)

                        #### + CTRY
                        reqind = "&facet=industry," + str(ind_code)

                        ## Working : Trial & error on address
                        #### CAN NOT USE PHONE2 - will result in ERROR ! ???? WTF
                        req1t ="http://api.linkedin.com/v1/company-search:(companies:(id,name,universal-name,status,website-url,twitter-id,\
industries,employee-count-range,num-followers,founded-year,description,specialties,locations:(address:(street1,street2,\
city,state,postal-code,country-code),contact-info:(phone1))))?format=json&facet=location,"

                        ## Working : Trial & error on address --- NO CTRY
                        req1nc ="http://api.linkedin.com/v1/company-search:(companies:(id,name,universal-name,status,website-url,twitter-id,\
industries,employee-count-range,num-followers,founded-year,description,specialties,locations:(address:(street1,street2,\
city,state,postal-code,country-code),contact-info:(phone1,phone2))))?format=json"


                        ###### ADDED Address2 amd Phone2
                        ###### record address location - 1st address is shown as HQ
                        ##### Format get_req for appropriate request
                        if ((mode == KEY_MODE)):
                                namekey = orig_keywords[ki]
                                indkey = ""
                                get_req = req1t + str(ctry) + reqe + reqkeyw + reqend
                                ##### FORMAT req for ALL countries if reqd
                                if ctry == "all:0":
                                        get_req = req1nc + reqe + reqkeyw + reqend
                                        pj.print_x("----- All CTRY Search ")
                                        
                        else:
                        #### This will be for case when mode is industry
                        ## Working : Trial & error on address
                                namekey = orig_keywords[ki]
                                indkey = str(ind_code)
                                get_req = req1t + str(ctry) + reqkeyw + reqind + reqend
                                ##### FORMAT req for ALL countries if reqd
                                if ctry == "all:0":
                                        get_req = req1nc + reqkeyw + reqind + reqend
                                        pj.print_x("----- All CTRY Search ")

                        ###### PRINT FINAL REQ
                        ### only print 
                        pj.print_x(get_req)
                        ####print(get_req)

                        ##### IF LOCAL COPY EXISTS USE IT
                        f_name = "P1_comp_" +  namekey + "_" + empl_code.replace(",","") + "_" + indkey + "_" + win_file_ctry + "_" + str(x / 20)+".txt"
                        f_path = raw_path
                        c_name = f_path + f_name
                        print("------------------------------------------------------------------------")
                        print(c_name)
                        local_source = False
                                
                        if os.path.isfile(c_name):
                                fdata = open(c_name)
                                page_data = json.load(fdata)
                                fdata.close()

                                if "companies" in page_data.keys():
                                        local_source = True
                                        print("1----LOCAL SRC")

                        ##### GET DATA FROM WEB
                        if not local_source:
                                print("2----LINKEDIN REQ")
                                resp,content = client.request(get_req,"GET","")
                                ##print(resp)
                                time.sleep(5)
                                print("2---RESP STATUS")
                                print(resp.status)
                                #print(x)       
                                ###print(x / 20)        
                                #print (content)


                                ### VALID response - resp.status >= 200 & <300
                                if resp.status >= 200 and resp.status < 300:
                                        ### Parse 
                                        page_data = json.loads(content)

                                        ########## WRITE FILE
                                        file = open(c_name, "w")
                                        file.write(content)
                                        file.close()


                                ##### If there is error then just print http resp
                                else:

                                        #### Error Files - only for reference
                                        file = open(c_name+"err", "w")
                                        file.write(content)
                                        file.close()

                                        print("!!!!! ERROR - contact your developer")
                                        print(resp)

                                        ### MAKE SURE Keyword loop also terminates
                                        ki = loopkeywords + 1
                                        
                                        #### end program - quit loop
                                        break

                        #### VALID RESPONSE from EITHER SOURCE             
                        if "companies" in page_data.keys():
                                #### MODIFY no_in_collection
                                no_in_collection = page_data["companies"]["_total"]
                                exact_no_in_collection = no_in_collection
                                
                                excess = no_in_collection % 20
                                ####print no_in_collection              
                                ####print mod20
                                ####remainder = no_in_collection - (20 * mod20)

                                if excess > 0:        
                                        no_in_collection = (no_in_collection-excess) + 20

                                ####if no_in_collection > 800:
                                ####    no_in_collection = 800           

                                start_page = 0
                                if "_start" in page_data["companies"].keys():
                                        start_page = page_data["companies"]["_start"]
                                                        
                                if no_in_collection > 0:
                                        print "Exact-Total : %d Rounded-Total : %d Current-Start : %d" %(exact_no_in_collection, no_in_collection, start_page)                       

                                        company_code, output_list = pj.process_linkedin_json(company_code, output_list, page_data, ki, orig_keywords[ki], ctry_map)
                                        acompany_code, aoutput_list = pj.process_linkedin_json_addr(acompany_code, aoutput_list, page_data, ki, ctry_map)

                                        if not fast_mode:
                                                #### KEEP WRITING INCREMENTAL FILES             
                                                pj.write_linkedin_output(output_list, file_prefix + "XLS_Company" + "_" + win_file_ctry + ".CSV",len(keywords),[])
                                                pj.write_linkedin_output(aoutput_list, file_prefix + "XLS_ADDR" + "_" + win_file_ctry + ".CSV",len(keywords),aoutput_list)                             


                                        #### Successful page receipt - goto NEXT page
                                        x = x + 20
                                        ##### Else there is no companies in response so ignore

                                else:
                                        print "-----Zero Companies Matched"

                                #### Write counts in a file
                                if gen_counts:
                                        genf.write(orig_keywords[ki])
                                        genf.write("-----" + str(exact_no_in_collection))
                                        genf.write(" \n")
                                        break


        ##ID1
        ##########
        ki = ki + 1

#### WRITE ONCE
if fast_mode:
        pj.write_linkedin_output(output_list, file_prefix + "XLS_Company" + "_" + win_file_ctry + ".CSV",len(keywords),[])
        pj.write_linkedin_output(aoutput_list, file_prefix + "XLS_ADDR" + "_" + win_file_ctry + ".CSV",len(keywords),aoutput_list)                         

##########
if gen_counts:
        genf.close()
