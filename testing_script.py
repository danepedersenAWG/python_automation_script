import json
import requests
import os
import copy
from copy import deepcopy
from bs4 import BeautifulSoup

import create_connection 



# Show instructions to user, define variables, and store user input. 
def user_prompt():
    json_files = []
    db_files = []
    for file in os.listdir(os.curdir):
        if file.endswith('db'):
            db_files.append(file)
        if file.endswith('.json'):
            json_files.append(file)
        
    if len(db_files) == 0:
        if len(json_files) == 0:
            print('********************')
            print('No .json file found')
            print('********************')
            print('')
            print('Please make sure that you have a .json file in this directory')
        else:
            read_json(json_files[0])
    else:
        if len(json_files) == 0:
            print('********************')
            print('No .json file found')
            print('********************')
            print('')
            print('Please make sure that you have a .json file in this directory')
        else:
            print('')
            print('')
            print('')
            print('***********************************************************')
            print('It looks like you already have a database in this directory.')
            print('***********************************************************')
            print('')
            print('Enter [1] to print all rows from db')
            print('')
            print('Enter [2] to overwrite db')
            print('')
            ui = input('1 or 2: ')
            if str(ui) == str(1):
                print('')
                print('Analyzing {0}...'.format(db_files[0]))
                print('')
                conn = create_connection.create_connection(db_files[0])
                create_connection.select_all_data(conn)

            elif str(ui) == str(2):
                os.remove(db_files[0])
                print('')
                print('Parsing {0}...'.format(json_files[0]))
                print('')
                read_json(json_files[0])
            else:
                user_prompt()



    


# Get Arcqa results and dev results and format them accordingly
def compare_arc_and_dev(f, i):
    # create session so we don't have to login on every request
    session_requests = requests.session()
    facilities_length = len(f)
    items_length = len(i)
    
    # get arc credentials so mine aren't exposed
    user_name = input('Username for Arc: ')
    password = input('Password for Arc : ')
    # arcqa uses a modal login, so we can get around this by passing credentials to the url 
    base_arc_qa = 'https://{0}:{1}@appqa.awginc.com/app/arc/'.format(user_name.strip(), password.strip())
    arc_session = session_requests.get(base_arc_qa)
    # check if status code is 200
    if(arc_session.ok):
        create_connection.create_db()
        database = './sqlite.db'
        conn = create_connection.create_connection(database)
        x = 0
        #don'twant to flood with requests during dev
        while x < facilities_length:
        #while x < 3:
            # get post response json
            post_dev_json_res = post_dev(f[x], i[x])
            #format an empty json object like the post response
            arc_data_to_compare = format_arc_json(post_dev_json_res)
            # get actual arc_data
            arc_item_url = '{0}gen/ItemQueryServlet?facility={1}&itemCode={2}&tableTarget=&pageName=ItemQueryResultSet'.format(base_arc_qa, f[x], i[x])
            page = session_requests.get(str(arc_item_url))
            soup = BeautifulSoup(page.content, 'html.parser')
            tds = soup.find_all('td')
            tds_len = len(tds)
            # 56
            if(tds_len > 0):
                try: 
                    with conn:
                        create_connection.add_all_data(conn, post_dev_json_res, arc_data_to_compare, tds)
                except:
                    print('There was an error saving the data to the database')
            x += 1
        print('Arc Data and API Data have been added to "sqlite.db" for storage and easy of use')
    else:
        print('Please check your username and password and try again')
        print(arc.status_code)
        

    
    return(f)
    


# Post to dev enpoints, and return data to be compared
def post_dev(f, i):
    session_requests = requests.session()
    # We use 3 APIs for ICQ: Item Hierarchy, Item List and Item Detail. 
    # Item Hierarchy - This one has no input and gets the information we use for the dropdowns(Facilities, Departments, Categories, Sub-Categories, Unique Groupings
    # Item List - This gets the list of items based on the user's search (MAX 500)
    # Item Detail - This gets the main detail and multiple subsections of data (Shipper info etc).Item Code and Facility are required
    base_sf_dev = 'https://sfapidev.awginc.com/'
    base_sf_dev_ip = 'https://10.1.200.75:5801'
    item_hierarchy_end = '{0}Item/ItemHierarchy'.format(base_sf_dev)
    item_list_end = '{0}Item/ItemList'.format(base_sf_dev)
    item_detail_end = '{0}Item/ItemDetail'.format(base_sf_dev)
    d = json.dumps({
        "Facility": "{0}".format(f),
        "Item_Code": "{0}".format(i),
        "Main_Info": "Y",
        "Store_Num": "",
        "Shipper_Info": "Y",
        "Deals_Info": "Y",
        "History_Info": "Y",
        "PO_Info": "Y",
        "Link_Info": "Y"
    })
    response = session_requests.post(item_detail_end,d)
    if(response.ok):
        sf_dev_res = response.content
        json_sf_dev_res = json.loads(sf_dev_res)
        return json_sf_dev_res['data']
    else:
        print('There was a {0} error when attempting to post to "{1}" with the data {2}'.format(response.status_code, item_detail_end, d))
        
        
# Function to take the sfdev response, and format an arc json string using it
def format_arc_json(post_dev_json_res):
    arc_data_to_compare = dict.fromkeys(post_dev_json_res)
    for key,value in post_dev_json_res.items():
        if(str(key) == 'FCLTY_CD'):
            arc_data_to_compare[key] = deepcopy(post_dev_json_res[key])
        elif(str(key) == 'ITEM_CD'):
            arc_data_to_compare[key] = deepcopy(post_dev_json_res[key])
        elif(str(key) == 'Main'):
            arc_data_to_compare[key] = dict.fromkeys(post_dev_json_res[key].keys())
            for key2,value2 in post_dev_json_res[key].items():
                if isinstance(value2, list):
                    if(arc_data_to_compare[key][key2] == None):
                        arc_data_to_compare[key][key2] = []
                    for i in value2:
                        mvmnt = deepcopy(i)
                        mvmnt['PAST_WKS_QTY_SHP'] = 'CHANGE'
                        arc_data_to_compare[key][key2].append(mvmnt)
                    
        elif(str(key) == 'Shipper_Comp'):
            if(len(post_dev_json_res[key]) > 0):
                dpcopy = deepcopy(post_dev_json_res[key])
                arc_data_to_compare[key] = dpcopy
        elif(str(key) == 'Deals'):
            if(len(post_dev_json_res[key]) > 0):
                dpcopy = deepcopy(post_dev_json_res[key])
                arc_data_to_compare[key] = dpcopy
        elif(str(key) == 'History'):
            if(len(post_dev_json_res[key]) > 0):
                dpcopy = deepcopy(post_dev_json_res[key])
                arc_data_to_compare[key] = dpcopy
        elif(str(key) == 'PO'):
            if(len(post_dev_json_res[key]) > 0):
                dpcopy = deepcopy(post_dev_json_res[key])
                arc_data_to_compare[key] = dpcopy   
        elif(str(key) == 'Link'):
            if(len(post_dev_json_res[key]) > 0):
                dpcopy = deepcopy(post_dev_json_res[key])
                arc_data_to_compare[key] = dpcopy
    return arc_data_to_compare


# Heavy lifting function 
def read_json(user_file):
    # Get a list of facilites and id
    facilities = []
    items = []
    # Start Try/Except
    try:
        file = open(user_file, 'r+')
        json_data = json.load(file)
        i = 0
        #first for loop gets the names of all of the objects and stores them globally
        for array_name in json_data:
            #loop through all objects, and add to corresponding dictionary if they exist
            for array_data in json_data[array_name]:
                if (array_data.__contains__('FACL_NUM') and array_data.__contains__('ITEM_CD')):
                    facilities.append(array_data['FACL_NUM']) 
                    items.append(array_data['ITEM_CD'])
                else:
                    print('ERROR: Please make sure all items have both "FACL_NUM" and "ITEM_CD" values and try again')

        if(len(facilities) == 0 or len(items) == 0):
            print('No Facilities or Item Codes found')
        else:
            compare_arc_and_dev(facilities, items)
        file.close()

    except (RuntimeError, TypeError, NameError) as err:
        print(err)

    

    

user_prompt()




