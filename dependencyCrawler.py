# -*- coding: utf-8 -*-
"""
Created on Sat Dec  8 07:32:14 2018

@author: AffanShemle
"""
import xml.etree.ElementTree as ET
import re
import os

whitespace_re = re.compile('[ \t\n\r\f\v]')
translation_table = dict.fromkeys(map(ord, '()][)'), None)

def get_main_tables_in_a_query(query,st_pos,table_ls):
    try:        
        while query.find('from',st_pos) != -1:
            from_starts = query.find('from',st_pos)
            i = from_starts + 4
            if whitespace_re.match(query[i]) == None: #it means that this is not a FROM clause but a FROM word inside a table or column name or something else
                st_pos = i
                continue
            else:
                table_name = ''
                while whitespace_re.match(query[i]) != None:
                    i = i + 1
                if query[i] == '(':
                    query = query[i:]
                    return get_main_tables_in_a_query(query,0,table_ls)
                else:
                    while whitespace_re.match(query[i]) == None:
                        table_name = table_name + query[i]
                        i = i + 1
                    table_ls.append(table_name)
                    st_pos = i
    except KeyboardInterrupt:
        print('Infinite Loop encountered')
    return table_ls

def get_joining_tables_in_a_query(query,st_pos,table_ls):
    try:        
        while query.find('join',st_pos) != -1:
            from_starts = query.find('join',st_pos)
            i = from_starts + 4
            if whitespace_re.match(query[i]) == None:  #it means that this is not a JOIN clause but a JOIN word inside a table or column name or something else
                st_pos = i
                continue
            else:
                table_name = ''
                while whitespace_re.match(query[i]) != None:
                    i = i + 1
                if query[i] == '(':
                    query = query[i:]
                    return get_joining_tables_in_a_query(query,0,table_ls)
                else:
                    while whitespace_re.match(query[i]) == None:
                        table_name = table_name + query[i]
                        i = i + 1
                    table_ls.append(table_name.upper())
                    st_pos = i
    except KeyboardInterrupt:
        print('Infinite Loop encountered')
    return table_ls

def get_all_query_source_tables(query_list): # a query is used in the source component
    table_ls = []
    try:        
        for query in query_list:
            query = wrangle_query(query)
            table_ls = table_ls + get_main_tables_in_a_query(query,0,[]) + get_joining_tables_in_a_query(query,0,[])
    except KeyboardInterrupt:
        print('Infinite Loop encountered')
    return table_ls

def wrangle_query(query): 
    query = '  ' + query + '  ' + '\n' #add some space in the beginning to fix the infinte loop if a comment line is the first line of the query and append a space to avoid handling the string out of index error, add a new line to prevent infinite loop if there is a single line comment in the end of the source query
    try:
        while query.find('/*',0) != -1:
            comnt_start = query.find('/*',0)
            comnt_end   = query.find('*/',comnt_start)
            query = query[:comnt_start - 1] + ' ' + query[comnt_end + 2:]
    except KeyboardInterrupt:
        print('Infinite Loop encountered')
    try:
        while query.find('--',0) != -1:
            comnt_start = query.find('--',0)
            comnt_end   = query.find("\n",comnt_start)
            query = query[:comnt_start - 1] + ' ' + query[comnt_end + 1:]
    except KeyboardInterrupt:
        print('Infinite Loop encountered')
    return query.lower()

def read_all_package_names(directory_location):
    list_of_pakages = os.listdir(directory_location)
    return list_of_pakages

def get_pkg_and_its_source_tables(directory_location,list_of_pakages):
    package_and_its_tables_dic = {}
    try:
        for package in list_of_pakages:
            query_list = get_all_queries(directory_location+'\\'+package)
            table_ls_1 = get_all_query_source_tables(query_list)
            table_ls_2 = get_all_direct_source_tables(directory_location+'\\'+package)
            table_ls = table_ls_1 + table_ls_2
            clean_table_ls = list(set(wrangle_tables(table_ls))) # remove duplicate table names from the list returned by wrangle_table func
            package_and_its_tables_dic.update({package:clean_table_ls})
    except KeyboardInterrupt:
        print('Infinite Loop encountered')
    return package_and_its_tables_dic

"""def wrangle_tables(table_ls):  # if your landing tables have other schemas apart fom DBO
    clean_table_ls = []
    for atable in table_ls:
        if atable.find('].',0) != -1:
            name_space = atable[1:atable.find('].',0)]
            if name_space.lower() == 'dbo':
                atable = atable[atable.find('].',0)+2:]
        if atable.find('].',0) == -1:
            atable = atable.translate(translation_table) 
        clean_table_ls.append(atable.upper())
    return clean_table_ls"""
    
def wrangle_tables(table_ls):  # if your landing tables have only one schemas DBO
    clean_table_ls = []
    for atable in table_ls:
        atable = atable.translate(translation_table)
        if atable.find('.',0) != -1:
            atable = atable[atable.find('.',0)+1:]
        clean_table_ls.append(atable.upper())
    return clean_table_ls
    
def get_all_direct_source_tables(package_loc):  # direct source table is selected from DB in the source component
    tree = ET.parse(package_loc)
    root = tree.getroot()
    table_list = []
    try:        
        for acomponent in root.iter('component'):
            access_mode = ''
            if acomponent.get('description') == 'OLE DB Source':
                
                for aproperty in acomponent.iter('property'):
                    
                    if aproperty.get("name") == 'AccessMode' and aproperty.text == '0':
                        access_mode = 'TableOrView'
                    
                    if aproperty.get("name") == 'AccessMode' and aproperty.text == '1':
                        access_mode = 'TableNameOrViewNameVariable'
                    
                for aproperty in acomponent.iter('property'): 
                    
                     if aproperty.get('name') == 'OpenRowset' and aproperty.text and access_mode == 'TableOrView':
                        table = aproperty.text
                        table_list.append(table)
                        
                     if aproperty.get('name') == 'OpenRowsetVariable' and aproperty.text and access_mode == 'TableNameOrViewNameVariable':
                        table_or_view_variable = aproperty.text
                        name_space = table_or_view_variable[0:table_or_view_variable.find('::',0)]
                        variable_name = table_or_view_variable[table_or_view_variable.find('::',0)+2:]
                        
                        for avariable in root.iter('{www.microsoft.com/SqlServer/Dts}Variable'):
                            
                            if avariable.get('{www.microsoft.com/SqlServer/Dts}Namespace') == name_space and avariable.get('{www.microsoft.com/SqlServer/Dts}ObjectName') == variable_name:
                                table = avariable.find('{www.microsoft.com/SqlServer/Dts}VariableValue').text
                                table_list.append(table)
    except KeyboardInterrupt:
        print('Infinite Loop encountered')
    return table_list

def get_all_queries(package_loc):
    tree = ET.parse(package_loc)
    root = tree.getroot()
    query_list = []
    try:        
        for acomponent in root.iter('component'):
            access_mode = ''
            if acomponent.get('description') == 'OLE DB Source':
                
                for aproperty in acomponent.iter('property'):
                    
                    if aproperty.get("name") == 'AccessMode' and aproperty.text == '2':
                        access_mode = 'SQLCommand'
                        
                    if aproperty.get("name") == 'AccessMode' and aproperty.text == '3':
                        access_mode = 'SQLCommandFromVariable'
                        
                for aproperty in acomponent.iter('property'):  
                    
                    if aproperty.get('name') == 'SqlCommand' and aproperty.text and access_mode == 'SQLCommand':
                        query = aproperty.text
                        query_list.append(query)
                    
                    if aproperty.get('name') == 'SqlCommandVariable' and aproperty.text and access_mode == 'SQLCommandFromVariable':
                        src_query_variable = aproperty.text
                        name_space = src_query_variable[0:src_query_variable.find('::',0)]
                        variable_name = src_query_variable[src_query_variable.find('::',0)+2:]
                        for avariable in root.iter('{www.microsoft.com/SqlServer/Dts}Variable'):
                            if avariable.get('{www.microsoft.com/SqlServer/Dts}Namespace') == name_space and avariable.get('{www.microsoft.com/SqlServer/Dts}ObjectName') == variable_name:
                                query = avariable.find('{www.microsoft.com/SqlServer/Dts}VariableValue').text
                                query_list.append(query)
    except KeyboardInterrupt:
        print('Infinite Loop encountered')
    return query_list

def get_dest_table_and_its_pckg_name(directory_location,list_of_pakages): # Handling Destination insertion using SQL Command is remaining
    dest_table_and_its_pckg_dic = {}
    for package in list_of_pakages:
        tree = ET.parse(directory_location+'\\'+package)
        root = tree.getroot()
        try:
            for acomponent in root.iter('component'):
                access_mode = ''
                
              
                if acomponent.get('description') == 'OLE DB Destination':
                    
                    for aproperty in acomponent.iter('property'):
                    
                        if aproperty.get("name") == 'AccessMode' and (aproperty.text == '0' or aproperty.text == '3'):
                            access_mode = 'TableOrView'
                        
                        if aproperty.get("name") == 'AccessMode' and (aproperty.text == '1' or aproperty.text == '4'):
                            access_mode = 'TableNameOrViewNameVariable'
                        
                    for aproperty in acomponent.iter('property'):
                        
                        if aproperty.get('name') == 'OpenRowset' and aproperty.text and access_mode == 'TableOrView':
                            dest_table = aproperty.text
                            dest_table = dest_table.translate(translation_table)
                            if dest_table.find('.',0) != -1:
                                dest_table = dest_table[dest_table.find('.',0)+1:]                             
                            dest_table_and_its_pckg_dic.update({dest_table.upper():package})
                        
                        if aproperty.get('name') == 'OpenRowsetVariable' and aproperty.text and access_mode == 'TableNameOrViewNameVariable':
                            table_or_view_variable = aproperty.text
                            name_space = table_or_view_variable[0:table_or_view_variable.find('::',0)]
                            variable_name = table_or_view_variable[table_or_view_variable.find('::',0)+2:]
                        
                            for avariable in root.iter('{www.microsoft.com/SqlServer/Dts}Variable'):
                            
                                if avariable.get('{www.microsoft.com/SqlServer/Dts}Namespace') == name_space and avariable.get('{www.microsoft.com/SqlServer/Dts}ObjectName') == variable_name:
                                    dest_table = avariable.find('{www.microsoft.com/SqlServer/Dts}VariableValue').text
                                    dest_table = dest_table.translate(translation_table)
                                    if dest_table.find('.',0) != -1:
                                        dest_table = dest_table[dest_table.find('.',0)+1:]  
                                    dest_table_and_its_pckg_dic.update({dest_table.upper():package})
                        
        except KeyboardInterrupt:
            print('Infinite Loop encountered')
    return dest_table_and_its_pckg_dic

def get_dependency_dml(directory_location, stage):
	dc_of_pckg_source_tables = {}
	dc_of_dest_table_pckg = {}
	pckg_to_populate_atable = ''
	ls_of_pckg_names = read_all_package_names(directory_location)
	dml_part1 = "INSERT [dbo].[PACKAGE_DEPENDENCY] ([PACKAGE_ID], [DEPENDENT_PACKAGE_ID]) VALUES ((SELECT PACKAGE_ID FROM PACKAGE WHERE NAME ='"
	dml_part2 = "' AND STAGE='"
	dml_part3 = "'),(SELECT PACKAGE_ID FROM PACKAGE WHERE NAME ='"
	dml_part4 = "' AND STAGE='"
	dml_part5 = "'))"
	#stage = 'MIL_REIN_OST'
	dc_of_pckg_source_tables = get_pkg_and_its_source_tables(directory_location,ls_of_pckg_names)
	dc_of_dest_table_pckg = get_dest_table_and_its_pckg_name(directory_location,ls_of_pckg_names)
	ls_of_dest_tables = list(dc_of_dest_table_pckg.keys())
	for package, ls_of_tables in dc_of_pckg_source_tables.items():
		for atable in ls_of_tables:
			ls_of_tables[:] = [atable for atable in ls_of_tables if atable in ls_of_dest_tables]
	for package, ls_of_tables in dc_of_pckg_source_tables.items():
		if len(ls_of_tables) != 0: # overwriting the dictionary key
			for atable in ls_of_tables:
				pckg_to_populate_atable = dc_of_dest_table_pckg[atable]
				if package != pckg_to_populate_atable:
					dml = dml_part1+pckg_to_populate_atable+dml_part2+stage+dml_part3+package+dml_part4+stage+dml_part5
					print(dml)
    return dc_of_pckg_dependency