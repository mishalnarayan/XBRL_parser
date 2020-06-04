import sys
import json
import csv
import os
import pandas as pd 
import time
max_field_length = 0
import csv
import glob
from datetime import datetime
start_time = datetime.now()
print start_time
import json
import ast
import tarfile
import zipfile
from bs4 import BeautifulSoup as soup
import copy
import multiprocessing
import re
sys.setrecursionlimit(10000)



df = pd.read_csv("/home/ven_agarg/metadata_files_missed.csv")
output_parsed_data_path = "/home/ven_unarayan/edgar_production_code_testing/2009_2019_parsed_data/10K/10K-2009/"
raw_data_path = "/dat/septa/mainline/edgar/10-K/2009/"


class FP:

    def __init__(self, path):
        self.path = path
        self.raw_data = []

    def collectAllDataFiles(self, path):
        """
        This function will create the data directory dictionary
        param path: Path of the dataset
        return: None
        """

        directory = [
                     os.path.join(
                                  path,
                                  filename
                     ) for filename in os.listdir(path) if 'docs' not in filename
        ]

        for file in directory:
            if os.path.isdir(file):
                #print 'Inside '+str(file)
                self.collectAllDataFiles(file)
            else:
                self.raw_data.append(file)

    def returnDd(self):
        return self.raw_data
#Recursive file listing function end





o = FP(raw_data_path)

o.collectAllDataFiles(raw_data_path) #/dat/wqtrialdata3/wqtrialdata/ujjawal/shivani_json

allfiless = o.returnDd()

#years_or_months_worked_upon = []

def doing_multiprocessing_stuff() :




    def identifier_extractor(keywords_to_find,open_file_object) :
        found_identifier = ""
        for current_line in open_file_object :
            if keywords_to_find in current_line :
                found_identifier = str(current_line.replace(keywords_to_find,"").strip().encode("utf-8","ignore"))
                break
        return found_identifier 


    # def worker_preprocessor(base_path,all_file_list,year_at_index) :
    #     final_output_dictionary = {}
    #     base_path_directories = os.listdir(base_path)
    #     no_of_year_data_in_dataset = list(set(list(map(lambda x: x.split("/")[year_at_index], all_file_list))))
        
    #     for single_year in no_of_year_data_in_dataset :
    #         if single_year not in base_path_directories :
    #             all_single_year_file_path = list(filter(lambda x: x.split("/")[year_at_index] == str(single_year), all_file_list))
    #             final_output_dictionary[single_year] = all_single_year_file_path
                
    #     return {list(final_output_dictionary.keys())[0] : final_output_dictionary[list(final_output_dictionary.keys())[0]] }

    def worker_preprocessor(base_path,all_file_list,year_at_index) :
        final_output_dictionary = {}
        no_of_year_data_in_dataset = list(set(list(map(lambda x: x.split("/")[year_at_index], all_file_list))))
        
        for single_year in no_of_year_data_in_dataset :
            if single_year not in years_or_months_worked_upon :
                all_single_year_file_path = list(filter(lambda x: x.split("/")[year_at_index] == str(single_year), all_file_list))
                final_output_dictionary[single_year] = all_single_year_file_path

        return final_output_dictionary

    def all_files_to_work_returner(worker_preprocsseor_dictionary) :
        print "here's your years_or_months_worked_upon_list " + str(years_or_months_worked_upon)
        for all_working_keys in list(worker_preprocsseor_dictionary.keys()) :
            if all_working_keys in years_or_months_worked_upon :
                pass
            else :
                print all_working_keys
                years_or_months_worked_upon.append(all_working_keys)
                current_working_files_from_raw_data = worker_preprocsseor_dictionary[all_working_keys]

                
                all_parsed_files_in_base_path = glob.glob(path_corrector(output_parsed_data_path) + all_working_keys + "/*")
                
                print all_parsed_files_in_base_path
                all_files_already_worked_upon = []
                for reading_current_file in all_parsed_files_in_base_path :
                    fp = open(reading_current_file, 'r').readlines()
                    for single_json_line in fp :
                        try :

                            value = json.load(single_json_line)
                        except :
                            value = json.loads(single_json_line)

                        all_files_already_worked_upon.append(value["filepath"])
                here_your_all_files = list(set(current_working_files_from_raw_data) - set(all_files_already_worked_upon))
                returning_dictionary_of_all_files = {all_working_keys : here_your_all_files}

                print here_your_all_files[0]
                print "filtered_files = " + str(len(here_your_all_files))
                print "total_files_in_dataset = " + str(len(current_working_files_from_raw_data))
                print "already_worked_files = " + str(len(set(all_files_already_worked_upon)))

                break
        return returning_dictionary_of_all_files 



    def path_corrector(input_path) :
        if str(input_path).endswith("/") :
            return str(input_path)
        else:
            return str(input_path) + "/"

    def from_to_finder(soup_object,tag,extraction_reference) :
        from_to =[] 
        print working_tag_dictionary[tag],extraction_reference
        if extraction_reference.lower() == "calculationarc" :
            all_found_data_elements = soup_object.find_all("link:calculationarc",{"xlink:to": re.compile(working_tag_dictionary[tag].replace(":","_"))}) 
            for current_found_data_elements in all_found_data_elements :
                from_to.append(current_found_data_elements["xlink:from"])

        if extraction_reference.lower() == "presentationarc" :
            all_found_data_elements = soup_object.find_all("link:presentationarc",{"xlink:to": re.compile(working_tag_dictionary[tag].replace(":","_"))}) 
            for current_found_data_elements in all_found_data_elements :
                from_to.append(current_found_data_elements["xlink:from"])

        return ",".join(from_to)

    def contextGrabber(soup_data,Contextid):
        context_tag = soup_data.find_all("xbrli:context", {"id" : Contextid})
        if len(context_tag) < 1 :
            context_tag = soup_data.find_all("context", {"id" : Contextid})
        context_output_dictionary = {}
        for currently_functional_tag in context_tag :
            #Start_date_end_date
            try :
                start_data_date = currently_functional_tag.find_all("xbrli:startdate")[0].get_text()
            except :
                try :
                    start_data_date = currently_functional_tag.find_all("startdate")[0].get_text()
                except :
                    start_data_date = ""
            context_output_dictionary["start_date"] = start_data_date
            try :
                end_data_date = currently_functional_tag.find_all("xbrli:enddate")[0].get_text()
            except :
                try :
                    end_data_date = currently_functional_tag.find_all("enddate")[0].get_text()
                except :
                    end_data_date = ""
            context_output_dictionary["end_date"] = end_data_date
            if end_data_date == "" and start_data_date == "" :
                try :
                    try :
                        instant_data_date = currently_functional_tag.find_all("period")[0].get_text()
                    except :
                        instant_data_date = currently_functional_tag.find_all("xbrli:period")[0].get_text()
                    context_output_dictionary["end_date"] = instant_data_date
                    context_output_dictionary["start_date"] = instant_data_date
                    del instant_data_date
                except :
                    pass
      
            #segment_filtering
            all_axis = []
            all_member = []
            for second_currently_working_tag in currently_functional_tag.find_all("xbrldi:explicitmember") :
                all_axis.append(second_currently_working_tag["dimension"])
                all_member.append(second_currently_working_tag.get_text())
            context_output_dictionary["Axis"] = ",".join(all_axis) 
            context_output_dictionary["Member"] = ",".join(all_member)
                
        return context_output_dictionary



    file_count = 0

    #file_list_dictionary = worker_preprocessor(output_parsed_data_path,allfiless,7)

    #allfiles = file_list_dictionary[list(file_list_dictionary.keys())[0]]

    file_list_dictionary = all_files_to_work_returner(worker_preprocessor(output_parsed_data_path,allfiless,8))
    
    allfiles = file_list_dictionary[list(file_list_dictionary.keys())[0]]

    print allfiles[0]
    print allfiles[-1]

    for file in allfiles:
        # try :

            data_output_directory_files_to_be_thrown = path_corrector(output_parsed_data_path) + list(file_list_dictionary.keys())[0] + "/" 
            if not os.path.exists(data_output_directory_files_to_be_thrown):
                os.makedirs(data_output_directory_files_to_be_thrown)
                
                
            print file
            read_data = open(file,"r")
            c_i_k = identifier_extractor("CENTRAL INDEX KEY:",read_data)
            company_name = identifier_extractor("COMPANY CONFORMED NAME:",open(file,"r"))
            announcement_date_or_filed_as_of_date = identifier_extractor("FILED AS OF DATE:",open(file,"r"))
            period_of_report = identifier_extractor("CONFORMED PERIOD OF REPORT:",open(file,"r"))
            date_as_of_change = identifier_extractor("DATE AS OF CHANGE:",open(file,"r"))
            
            xml_soup = soup(open(file,"r").read(),"lxml")
            
            alltags = list(set([tag.name for tag in xml_soup.find_all()]))
            working_tag = []
            working_tag_dictionary = {}
            for individual_tag in alltags :
                
                individual_tag = individual_tag.lower()
                for x in df["name"] :
                    x_normal_case = copy.copy(x)
                    x = x.lower()   
                    if x == individual_tag : #can change here to in if needed for partial match
                        working_tag.append(individual_tag)
                        working_tag_dictionary[individual_tag] = x_normal_case
            working_tag = list(set(working_tag))


            try :
                if len(working_tag) < 1 : # logging if no given tags are present in filings
                    with open("/home/ven_unarayan/edgar_production_code_testing/2009_2019_parsed_data/10K/log_2009.txt","a") as m1 :
                        m1.write(str(file)  + "\n")
                        print "log written"
            except :
                pass
            
            for single_tag in working_tag :
                # try :

                    tag_data = xml_soup.find_all(single_tag)
                    #print len(tag_data)
                    if len(tag_data) > 0 :
                        for x in tag_data :
                                                   
                            all_data = {}
                            
                            for single_attribute in  x.attrs.keys() :
                                #print single_attribute, x[single_attribute]
                                all_data[single_attribute] = (x[single_attribute])
                                         
                            
                            all_textual_data = []
                            try :
                                
                                for l in soup(soup(str(x),"xml").get_text(),"lxml").find_all() : 
                                    #print l.get_text()
                                    current_data = l.get_text()
                                    if current_data not in all_textual_data :
                                        all_textual_data.append((current_data))
                            except :
                                pass
                                
                            #all_data.append(str(x.get_text().strip()))
                            
                            #print " ".join(all_textual_data)
                            all_textual_data = " ".join(all_textual_data)
                            
                            
                            if len(all_textual_data) < 2 :
                                #print "you fucked up"
                                all_textual_data = ""
                                #print single_tag
                                try :                    
                                    all_textual_data = soup(soup(str(x),"xml").get_text(),"lxml").get_text()
                                except :
                                    try :
                                        all_textual_data = []
                                        for l in soup(x.get_text(),"lxml").find_all() : 
                                            current_data = l.get_text()
                                            if current_data not in all_textual_data :
                                                all_textual_data.append((current_data))
                                        all_textual_data = " ".join(all_textual_data)
                                    except :
                                        print "SHIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIITTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
                                        all_textual_data = x.get_text()
                                

                              
                            all_data["content"] = all_textual_data.encode("utf-8","replace").replace("'","")
                            all_data["company_conformed_name"] = (company_name)
                            all_data["central_index_key"] = (c_i_k)
                            all_data["announcement_date_or_filed_as_of_date"] = str(announcement_date_or_filed_as_of_date)
                            all_data["period_of_report"] = str(period_of_report)
                            all_data["date_as_of_change"] = str(date_as_of_change)
                            all_data["filepath"] = (str(file))
                            all_data["calculation_from"] = from_to_finder(xml_soup,single_tag,"calculationarc")
                            all_data["presentation_from"] = from_to_finder(xml_soup,single_tag,"presentationarc")
                            
                            print all_data["calculation_from"]
                            print all_data["presentation_from"]

                            print file
                            print all_data["contextref"]
                            print single_tag

                            print contextGrabber(xml_soup,all_data["contextref"])

                            all_data.update(contextGrabber(xml_soup,all_data["contextref"]))
                            
                            #print str(x.get_text().strip())
                            with open( data_output_directory_files_to_be_thrown + str(working_tag_dictionary[single_tag]).replace(":","__") + ".json","a") as o1:
                                print "written"
                                o1.write(str(json.dumps(all_data)) + "\n")

                # except :
                #     pass
        # except :
        #     pass
            file_count = file_count + 1
            percent_calci = (float(file_count)/float(len(allfiles)))*100.0
            print "working on file no " + str(file_count) + " of total file " + str(len(allfiles)) + " " + str(percent_calci) + " % completed"        
    

processes = []
print(multiprocessing.cpu_count())
from multiprocessing import Process, Manager
with Manager() as manager: 
    years_or_months_worked_upon = manager.list()
    for _ in range(31) :
        p = multiprocessing.Process(target = doing_multiprocessing_stuff)
        p.start()
        time.sleep(30)
        processes.append(p)
    for process in processes:
        time.sleep(30)
        process.join()


end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))





