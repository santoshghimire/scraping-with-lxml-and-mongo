from cStringIO import StringIO
import urllib
import csv
from lxml import etree     
import lxml.html

from bson.objectid import ObjectId

import pymongo
from pymongo import MongoClient
from pymongo import Connection
import json
from bson import BSON
from bson import json_util


class MongoConnection():
    '''Class to create connection with mongoDB'''
    def __init__ (self, host="localhost",port=27017, db_name='indexer', conn_type="local"):
        self.host = host
        self.port = port
        self.conn = Connection(self.host, self.port)
        self.db = self.conn[db_name]
        # Uncomment the next line if you have authentication in place for mongodb database.
        # self.db.authenticate(username, password)
    
    def create_table(self, table_name, index=None):
        self.db[table_name].create_index( [(index, pymongo.DESCENDING)] )

    def insert_one(self, table_name, value):
        self.db[table_name].insert(value)    

    def delete_all(self, table_name):
        self.db[table_name].remove({})

class ScraperData():
    """Class to save scraped data to mongodb"""
    def __init__(self):
        self.db_object = MongoConnection("localhost",27017,'scrapedata')
        self.table_name = 'numpy'
        self.db_object.create_table(self.table_name,'main_topic')

    def write_data(self, table_name, value):
        self.db_object.insert_one(table_name, value)

    def remove_all(self, table_name):
        self.db_object.delete_all(table_name)


class SiteCrawler():
    '''Class that does the scraping task'''
    def __init__(self):
        self.url_list = ['http://docs.scipy.org/doc/numpy/reference/', 'http://docs.scipy.org/doc/scipy/reference/',
                    'http://docs.scipy.org/doc/numpy/user/']
        self.file_list = ['numpy', 'scipy', 'numpyGuide']
        self.url = 'http://docs.scipy.org/doc/numpy/reference/'
        self.table_name = 'numpy'
        self.parser = etree.HTMLParser()
        self.mongo_obj = ScraperData()

    def get_function_details(self, func_details, topics):
        html = urllib.urlopen(func_details['function_link'])
        html = html.read()
        self.parser = etree.HTMLParser()
        maintree   = etree.parse(StringIO(html), self.parser)
        mainContent1 = maintree.xpath("//dl[@class='method']")     #scrape main div containing data
        mainContent2 = maintree.xpath("//dl[@class='function']")     #scrape main div containing data
        if len(mainContent1)==0 and len(mainContent2)!=0:
            mainContent = mainContent2
        elif len(mainContent2)==0 and len(mainContent1)!=0:
            mainContent = mainContent1
        elif len(mainContent1)==0 and len(mainContent2)==0:
            return
        argument_list = [ child for child in mainContent[0].iterchildren('dt') ]      #get its child dt    
        contentHTML= (etree.tostring(argument_list[0], pretty_print=True))
        tree   = etree.parse(StringIO(contentHTML), self.parser)
        argument_list = tree.xpath("//text()")
        argument_list = ''.join(argument_list[1:len(argument_list)-1]).encode('utf-8').strip()
        
        # getting details for each args
        split_data = argument_list.split('(')
        full_function_name = split_data[0]
        sec_split_data = split_data[1].split(')')
        args = sec_split_data[:-1]
        arg_dict = {}
        if len(args)!=0:
            args = args[0].split(',')
            for each_arg in args:
                each_split = each_arg.split('=')
                if len(each_split)==1:
                    if each_arg.find('.')== -1:
                        arg_dict[each_arg] = {'optional_flag': 0, 'default_value': ''}
                else:
                    if each_split[0].find('.')== -1:
                        arg_dict[each_split[0]] = {'optional_flag': 1, 'default_value': each_split[1]}

        # parsing examples
        examples = ''
        dd =  [ child for child in mainContent[0].iterchildren('dd') ]      #get its child dd
        example_div =  [ child for child in dd[0].iterchildren('div') ]      #get its child div
        if len(example_div)!=0:
            contentHTML= (etree.tostring(example_div[0], pretty_print=True))
            tree   = etree.parse(StringIO(contentHTML), self.parser)
            example_div_class = tree.xpath("//@class")
            if example_div_class[0] == 'highlight-python':
                examples = tree.xpath("//text()")
                examples = ''.join(examples)

        parameters_table = [ child for child in mainContent[0].iterdescendants('table') ]      #get its child table
        if len(parameters_table)!=0:
            contentHTML= (etree.tostring(parameters_table[0], pretty_print=True))
            tree   = etree.parse(StringIO(contentHTML), self.parser)
            table_class = tree.xpath("//@class")
            if table_class[0] == 'docutils field-list':
                all_desc = [ child for child in parameters_table[0].iterdescendants('tr') ]      #get its child tr            
                # for parameters
                argument_desc = [ child for child in all_desc[0].iterchildren('td') ]      #get its child td       
                contentHTML= (etree.tostring(argument_desc[0], pretty_print=True))
                tree   = etree.parse(StringIO(contentHTML), self.parser)
                argument_desc_list = tree.xpath("//text()")
                para_arg={}
                para_arg['argument_desc'] = ''.join(argument_desc_list).encode('utf-8').strip()
                # for returns
                if len(all_desc) == 2:
                    parameter_desc = [ child for child in all_desc[1].iterchildren('td') ]      #get its child td
                    contentHTML= (etree.tostring(parameter_desc[0], pretty_print=True))
                    tree   = etree.parse(StringIO(contentHTML), self.parser)
                    parameter_desc_list = tree.xpath("//text()")
                    para_arg['parameter_desc'] = ''.join(parameter_desc_list).encode('utf-8').strip()
                para_arg['parameter_desc'] = para_arg.get('parameter_desc') if para_arg.get('parameter_desc')!=None else ''

                # final_data = {'function_name':func_details['function_name'],
                final_data = {'function_name':full_function_name,
                            'function_link':func_details['function_link'],
                            'function_description':func_details['function_desc'],
                            'argument_list':arg_dict,
                            'argument_description':para_arg['argument_desc'],
                            'return_parameter':para_arg['parameter_desc'],
                            'examples': examples,
                            'sub_topic':topics['sub_topic'],
                            'sub_topic_link':topics['sub_topic_link'],
                            'main_topic':topics['main_topic'],
                            'main_topic_link':topics['main_topic_link']}
                #write to mongodb
                self.mongo_obj.write_data(self.table_name, final_data)


        else:
            final_data = {'function_name':full_function_name,
                            'function_link':func_details['function_link'],
                            'function_description':func_details['function_desc'],
                            'argument_list':arg_dict,
                            'argument_description':'',
                            'return_parameter':'',
                            'examples': examples,
                            'sub_topic':topics['sub_topic'],
                            'sub_topic_link':topics['sub_topic_link'],
                            'main_topic':topics['main_topic'],
                            'main_topic_link':topics['main_topic_link']}
            self.mongo_obj.write_data(self.table_name, final_data)


    def get_func_tables(self, each_topic, topics):
        # check if table of functions exists
        function_table = [ child for child in each_topic.iterchildren('table') ]      #get its child table
        if len(function_table)!= 0:
            contentHTML= (etree.tostring(function_table[0], pretty_print=True))
            tree   = etree.parse(StringIO(contentHTML), self.parser)
            table_class = tree.xpath("//@class")
            if table_class[0] == "longtable docutils":
                all_trs = [ child for child in function_table[0].iterdescendants('tr') ]      #get its child tr
                for each_tr in all_trs:
                    all_tds = [ child for child in each_tr.iterchildren('td') ]      #get its child td
                    func_details = {}
                    function_a = [ child for child in all_tds[0].iterchildren('a') ]      #get its child a
                    if len(function_a)!=0:
                        contentHTML= (etree.tostring(function_a[0], pretty_print=True))
                        tree   = etree.parse(StringIO(contentHTML), self.parser)
                        function_link = tree.xpath("//@href")[0].encode('utf-8').strip()
                        function_name = tree.xpath("//tt//span//text()")[0].encode('utf-8').strip()
                        func_details['function_name'] = function_name
                        func_details['function_link'] = self.url+function_link
                    
                    contentHTML= (etree.tostring(all_tds[1], pretty_print=True))
                    tree   = etree.parse(StringIO(contentHTML), self.parser)
                    function_desc = tree.xpath("//text()")
                    if len(function_desc)!=0:
                        func_details['function_desc'] = function_desc[0].encode('utf-8').strip()
                    func_details['function_desc'] = func_details.get('function_desc') if func_details.get('function_desc')!=None else ''
                    if func_details.get('function_link')!=None:
                        self.get_function_details(func_details, topics)


    def scrape_section(self, element, topics, scipy_first=False, all_info=None):
        if scipy_first:
            h1_topic = [ child for child in element.iterchildren('h1') ]      #get its child h1    
            actual_link = [ child for child in h1_topic[0].iterchildren('a') ]      #get its child a
            if len(actual_link)==2:
                contentHTML= (etree.tostring(actual_link[0], pretty_print=True))
                tree   = etree.parse(StringIO(contentHTML), self.parser)
                actual_link = tree.xpath("//@href")[0].split('/')
                if actual_link[0]== '..':
                    html = urllib.urlopen(self.url + actual_link[1])
                    html = html.read()
                    maintree   = etree.parse(StringIO(html), self.parser)
                    mainContent = maintree.xpath("//div[@class='section']")     #scrape main div containing data
                    self.scrape_section(mainContent[0], topics)
            else:
                return    
        else:        
            main_topics = [ child for child in element.iterchildren('div') ]      #get its child div
            for each_topic in main_topics:
                contentHTML= (etree.tostring(each_topic, pretty_print=True))
                tree   = etree.parse(StringIO(contentHTML), self.parser)
                div_class = tree.xpath("//@class")
                if div_class[0] == 'section':
                    title = [ child for child in each_topic.iterchildren('h2') ]      #get its child h2
                    mini_title, information='',''
                    if len(title)==0:
                        title = [ child for child in each_topic.iterchildren('h3') ]      #get its child h3
                    if len(title)!=0:
                        titleHTML= (etree.tostring(title[0], pretty_print=True))
                        title_tree   = etree.parse(StringIO(titleHTML), self.parser)
                        mini_title = title_tree.xpath("//text()")[0].encode('utf-8').strip()
                    if self.url == 'http://docs.scipy.org/doc/numpy/user/':
                        info = [ child for child in each_topic.iterchildren('p') ]      #get its child para
                        if len(info)!=0:
                            infoHTML= (etree.tostring(info[0], pretty_print=True))
                            info_tree   = etree.parse(StringIO(infoHTML), self.parser)
                            information = info_tree.xpath("//text()")[0].encode('utf-8').strip()
                            if all_info!=None:
                                info_details = {'mini_title': mini_title, 'mini_info': information,
                                'parent_title': all_info.get('mini_title'), 'parent_info': all_info.get('mini_info')}
                            else:
                                info_details = {'mini_title': mini_title, 'mini_info': information}
                        else:
                            info_details = {'mini_title': mini_title, 'mini_info': information}
                        self.scrape_section(each_topic, topics, all_info=info_details)

                    else:
                        self.get_func_tables(each_topic, topics)     # check if table of functions exists
                        # check if there is a section div within the div
                        self.scrape_section(each_topic, topics)   
                else:
                    if self.url == 'http://docs.scipy.org/doc/numpy/user/' and all_info!=None:
                        final_data = {'sub_topic':topics['sub_topic'],
                                    'sub_topic_link':topics['sub_topic_link'],
                                    'main_topic':topics['main_topic'],
                                    'main_topic_link':topics['main_topic_link']}
                        if all_info.get('parent_title')==None and all_info.get('parent_info')==None:
                            final_data['parent_title'] = all_info['mini_title']
                            final_data['parent_info'] = all_info['mini_info']
                            final_data['mini_title'] = ''
                            final_data['mini_info'] =''

                            self.mongo_obj.write_data(self.table_name, final_data)
                        else:
                            final_data['parent_title'] = all_info.get('parent_title')
                            final_data['parent_info'] = all_info.get('parent_info')
                            final_data['mini_title'] = all_info['mini_title']
                            final_data['mini_info'] =all_info['mini_info']

                            self.mongo_obj.write_data(self.table_name, final_data)


    def get_all_functions(self, passedurl, topics):
        '''open the function page for parsing'''
        html = urllib.urlopen(passedurl)
        html = html.read()
        maintree   = etree.parse(StringIO(html), self.parser)
        mainContent = maintree.xpath("//div[@class='section']")     #scrape main div containing data
        if self.url=='http://docs.scipy.org/doc/scipy/reference/':
            self.scrape_section(mainContent[0], topics, scipy_first=True)
        else:
            self.scrape_section(mainContent[0], topics)

    def main(self):
        '''Scrapes function name, argument list, description for argument, URL for description, URL for examples.'''
        html = urllib.urlopen(self.url)
        html = html.read()
        maintree   = etree.parse(StringIO(html), self.parser)
        mainContent = maintree.xpath("//div[@class='section']")     #scrape main div containing data

        main_h1 = [ child for child in mainContent[0].iterchildren('h1') ]      #get its child h1
        contentHTML= (etree.tostring(main_h1[0], pretty_print=True))
        tree   = etree.parse(StringIO(contentHTML), self.parser)
        title_text = tree.xpath("//text()")[0].strip()      #title_text

        all_content = [ child for child in mainContent[0].iterchildren('div') ]     # get its child div
        contentHTML= (etree.tostring(all_content[0], pretty_print=True))
        tree   = etree.parse(StringIO(contentHTML), self.parser)
        all_content_class = tree.xpath("//@class")[0].strip()      
        if all_content_class=='toctree-wrapper compound':
            main_ul = [ child for child in all_content[0].iterchildren('ul') ]      #get its child ul    
        else:
            main_ul = [ child for child in all_content[1].iterchildren('ul') ]      #get its child ul

        main_li = [ child for child in main_ul[0].iterchildren('li') ]      #get its child li
        for each_li in main_li:
            main_a = [ child for child in each_li.iterchildren('a') ]      #get its child a
            sectionHTML= (etree.tostring(main_a[0], pretty_print=True))
            tree   = etree.parse(StringIO(sectionHTML), self.parser)
            main_topic = ' '.join(tree.xpath("//text()")).encode('utf-8').strip()
            main_topic_link = tree.xpath("//@href")[0].encode('utf-8').strip()
            # main_topic, main_topic_link

            sub_ul = [ child for child in each_li.iterchildren('ul') ]      #get its child ul
            if len(sub_ul)!=0:
                sub_li = [ child for child in sub_ul[0].iterchildren('li') ]      #get its children li
                for each_sub_li in sub_li:
                    sectionHTML= (etree.tostring(each_sub_li, pretty_print=True))
                    tree   = etree.parse(StringIO(sectionHTML), self.parser)
                    sub_topic = ' '.join(tree.xpath("//text()")).encode('utf-8').strip()
                    sub_topic_link = tree.xpath("//@href")[0].encode('utf-8').strip()
                    topics = {'main_topic': main_topic, 'main_topic_link': self.url+main_topic_link,
                    'sub_topic': sub_topic, 'sub_topic_link': self.url+sub_topic_link}
                    self.get_all_functions(topics['sub_topic_link'], topics)
            else:
                topics = {'main_topic': main_topic, 'main_topic_link': self.url+main_topic_link,
                    'sub_topic': '', 'sub_topic_link': ''}
                self.get_all_functions(topics['main_topic_link'], topics)

    def start_scraping(self):
        for count, url in enumerate(self.url_list):
            self.url = url
            self.table_name = self.file_list[count]
            # clear the collection
            self.mongo_obj.remove_all(self.table_name)
            self.main()

crawler = SiteCrawler()
crawler.start_scraping()

# MONGODUMP
# SYNTAX: mongodump --db=<db_name> --collection=<collection_name> --out=data/
# EXAMPLE: mongodump --db='scrapedata' --collection='numpy' --out=data/

#MONGORESTORE
# SYNTAX: mongorestore --db=<db_name> --collection=<collection_name> data/<db_name>/<collection_name>.bson
# EXAMPLE: mongorestore --db='scrapedata' --collection='numpy' data/scrapedata/numpy.bson
