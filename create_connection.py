import os
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_db():
    database = r"./sqlite.db"

    sql_create_arc_and_dev_data_table= """CREATE TABLE IF NOT EXISTS arc_and_dev_data (
        id integer PRIMARY KEY,
        arc_or_dev text NOT NULL,
        FCLTY_CD text NOT NULL,
        ITEM_CD text NOT NULL,
        ITEM_LONG_NM text,
        RTL_UNIT_SIZE_DS text,
        BSP_CRNT_AM text,
        ITEM_UPC_CD text,
        CASE_UPC_CD text,
        RTL_CITY_CRNT_GRS_PRFT_PC text,
        RTL_RRL_CRNT_GRS_PRFT_PC text,
        SELL_AT_WGT text,
        INVNTRY_DPRTMNT_CD text,
        ITEM_STATUS text,
        CTGRY_NB text,
        ITEM_STP_DT text,
        DSCNTND_DT text,
        BUY_NAM text,
        AWG_SBSTTTN_ITEM_CD text,
        WRHS_PALLET_QT text,
        LGCL_ORDR_QT text,
        CURRENT_INVENTORY text,
        GRP_LED_TM text,
        WRHS_CD text,
        ITM_DSC text,
        BRND_NAME_DS text,
        STR_PACK_QT text,
        MSTR_PACK_QT text,
        SBSTTTN_ITEM_TYPE_CD text,
        RTL_CITY_CRNT_AM text,
        RTL_RRL_CRNT_AM text,
        INNER_PACK_CD text,
        ITEM_UPC_CMPRSD_CD text,
        RCLMTN_ELGBL_FL text,
        PRCHS_VNDR_DS text,
        POS_SPCL_ATHRZTN_FL text,
        RTL_UNT_INR_CSE text,
        SUB_CTGRY_NB text,
        CTGRY_MNGR_CD text,
        BUY_EMAIL text,
        BUY_TEL_NBR text,
        CAT_MGR_NAM text,
        CAT_MGR_EMAIL text,
        CAT_MGR_TEL_NBR text,
        NEXT_AVLBL_DT text,
        HZRDS_MTRL_CD text,
        CNTRY_OF_ORGN_DS text,
        ITEM_RNKNG_NB text,
        NMBR_ITEM_RNKNG_QT text,
        UNIQUE_GROUPING_NBR text,
        UNIQUE_GROUPING_DESC text,
        ONE_WEEK_MVMNT_START_DT text,
        ONE_WEEK_MVMNT_END_DT text,
        ONE_WEEK_MVMNT_PAST_WKS_QTY_SHP text,
        TWO_WEEK_MVMNT_START_DT text,
        TWO_WEEK_MVMNT_END_DT text,
        TWO_WEEK_MVMNT_PAST_WKS_QTY_SHP text,
        SIX_MONTH_AVG_MVMNT_START_DT text,
        SIX_MONTH_AVG_MVMNT_END_DT text,
        SIX_MONTH_AVG_MVMNT_PAST_WKS_QTY_SHP text
    );"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create arc table
        create_table(conn, sql_create_arc_and_dev_data_table)

    else:
        print("Error! cannot create the database connection.")


def create_row(conn, data):
    """
    Write a new record into the table you specify
    :param conn:
    :param data:
    
    :return: data id
    """
    sql = ''' INSERT INTO arc_and_dev_data(arc_or_dev,FCLTY_CD,ITEM_CD,ITEM_LONG_NM,RTL_UNIT_SIZE_DS,RTL_UNT_INR_CSE,BSP_CRNT_AM,ITEM_UPC_CD,CASE_UPC_CD,RTL_CITY_CRNT_GRS_PRFT_PC,RTL_RRL_CRNT_GRS_PRFT_PC,SELL_AT_WGT,INVNTRY_DPRTMNT_CD,ITEM_STATUS,CTGRY_NB,ITEM_STP_DT,DSCNTND_DT,BUY_NAM,AWG_SBSTTTN_ITEM_CD,WRHS_PALLET_QT,LGCL_ORDR_QT,CURRENT_INVENTORY,GRP_LED_TM,ONE_WEEK_MVMNT_START_DT,ONE_WEEK_MVMNT_END_DT,ONE_WEEK_MVMNT_PAST_WKS_QTY_SHP,TWO_WEEK_MVMNT_START_DT,TWO_WEEK_MVMNT_END_DT,TWO_WEEK_MVMNT_PAST_WKS_QTY_SHP,SIX_MONTH_AVG_MVMNT_START_DT,SIX_MONTH_AVG_MVMNT_END_DT,SIX_MONTH_AVG_MVMNT_PAST_WKS_QTY_SHP)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
              
                        
    cur = conn.cursor()
    cur.execute(sql, data)
    conn.commit()
    print('Adding new row...')
    return cur.rowcount

def add_all_data(conn, post_dev_json_res, arc_data_to_compare, tds):
    """
    Do all parsing, appending, and posting to db
    :param conn:
    :param post_dev_json_res:
    :param arc_data_to_compare:
    :param tds:
    
    :return: result of execution
    """
    # https://appqa.awginc.com/app/arc/gen/ItemQueryServlet?facility=02&itemCode=0022848&tableTarget=&pageName=ItemQueryResultSet%27
    # pack = master back divided by store pack.
    # Do a check when data comes back to see if master pack divided by store pack = pack
    # RTL_UNT_INR_CSE = MSTR_PACK_QT % STR_PACK_QT
    arc_main = arc_data_to_compare['Main']
    arc_main['ITEM_MVNT'][0]['PAST_WKS_QTY_SHP'] = tds[60].text.strip()
    arc_main['ITEM_MVNT'][1]['PAST_WKS_QTY_SHP'] = tds[64].text.strip()
    arc_main['ITEM_MVNT'][2]['PAST_WKS_QTY_SHP'] = 'NOT ON ARC'
    #START DB ENTRIES
    arc_data_to_compare['FCLTY_CD'] = post_dev_json_res['FCLTY_CD']
    arc_data_to_compare['ITEM_CD'] = tds[9].text.strip()
    arc_main['ITEM_LONG_NM'] = tds[1].text.strip()
    arc_main['RTL_UNIT_SIZE_DS'] = tds[3].text.strip()
    arc_main['RTL_UNT_INR_CSE'] = tds[7].text.strip()
    arc_main['BSP_CRNT_AM'] = tds[11].text.strip()
    arc_main['ITEM_UPC_CD'] = tds[13].text.strip()
    arc_main['CASE_UPC_CD'] = tds[17].text.strip()
    arc_main['RTL_CITY_CRNT_GRS_PRFT_PC'] = tds[19].text.strip()
    arc_main['RTL_RRL_CRNT_GRS_PRFT_PC'] = tds[19].text.strip()
    arc_main['SELL_AT_WGT'] = tds[23].text.strip()
    arc_main['INVNTRY_DPRTMNT_CD'] = tds[25].text.strip()
    arc_main['ITEM_STATUS'] = tds[27].text.strip()
    arc_main['CTGRY_NB'] = tds[29].text.strip()
    arc_main['ITEM_STP_DT'] = tds[31].text.strip()
    arc_main['DSCNTND_DT'] = tds[35].text.strip()
    arc_main['BUY_NAM'] = tds[41].text.strip()
    arc_main['AWG_SBSTTTN_ITEM_CD'] = tds[43].text.strip()
    arc_main['WRHS_PALLET_QT'] = tds[47].text.strip()
    arc_main['LGCL_ORDR_QT'] = tds[51].text.strip()
    arc_main['CURRENT_INVENTORY'] = tds[55].text.strip()
    arc_main['GRP_LED_TM'] = tds[86].text.strip()
    # ONE_WEEK_MVMNT_START_DT = arc_main['ITEM_MVNT'][0]['WK_START_DT']
    # ONE_WEEK_MVMNT_END_DT = arc_main['ITEM_MVNT'][0]['WK_END_DT']
    # ONE_WEEK_MVMNT_PAST_WKS_QTY_SHP = arc_main['ITEM_MVNT'][0]['PAST_WKS_QTY_SHP']
    # TWO_WEEK_MVMNT_START_DT = arc_main['ITEM_MVNT'][1]['WK_START_DT']
    # TWO_WEEK_MVMNT_END_DT = arc_main['ITEM_MVNT'][1]['WK_END_DT']
    # TWO_WEEK_MVMNT_PAST_WKS_QTY_SHP = arc_main['ITEM_MVNT'][1]['PAST_WKS_QTY_SHP']
    # SIX_MONTH_AVG_MVMNT_START_DT = 'NOT ON ARC'
    # SIX_MONTH_AVG_MVMNT_END_DT = 'NOT ON ARC'
    # SIX_MONTH_AVG_MVMNT_PAST_WKS_QTY_SHP = 'NOT ON ARC'

    #Specify arc and dev data and put it into db
    arc_data = []
    arc_data.append('a')
    arc_data.append(arc_data_to_compare['FCLTY_CD'])
    arc_data.append(arc_data_to_compare['ITEM_CD'])
    arc_data.append(arc_main['ITEM_LONG_NM'])
    arc_data.append(arc_main['RTL_UNIT_SIZE_DS'])
    arc_data.append(arc_main['RTL_UNT_INR_CSE'])
    arc_data.append(arc_main['BSP_CRNT_AM'])
    arc_data.append(arc_main['ITEM_UPC_CD'])
    arc_data.append(arc_main['CASE_UPC_CD'])
    arc_data.append(arc_main['RTL_CITY_CRNT_GRS_PRFT_PC'])
    arc_data.append(arc_main['RTL_RRL_CRNT_GRS_PRFT_PC'])
    arc_data.append(arc_main['SELL_AT_WGT'])
    arc_data.append(arc_main['INVNTRY_DPRTMNT_CD'])
    arc_data.append(arc_main['ITEM_STATUS'])
    arc_data.append(arc_main['CTGRY_NB'])
    arc_data.append(arc_main['ITEM_STP_DT'])
    arc_data.append(arc_main['DSCNTND_DT'])
    arc_data.append(arc_main['BUY_NAM'])
    arc_data.append(arc_main['AWG_SBSTTTN_ITEM_CD'])
    arc_data.append(arc_main['WRHS_PALLET_QT'])
    arc_data.append(arc_main['LGCL_ORDR_QT'])
    arc_data.append(arc_main['CURRENT_INVENTORY'])
    arc_data.append(arc_main['GRP_LED_TM'])
    arc_data.append(arc_main['ITEM_MVNT'][0]['WK_START_DT'])
    arc_data.append(arc_main['ITEM_MVNT'][0]['WK_END_DT'])
    arc_data.append(arc_main['ITEM_MVNT'][0]['PAST_WKS_QTY_SHP'])
    arc_data.append(arc_main['ITEM_MVNT'][1]['WK_START_DT'])
    arc_data.append(arc_main['ITEM_MVNT'][1]['WK_END_DT'])
    arc_data.append(arc_main['ITEM_MVNT'][1]['PAST_WKS_QTY_SHP'])
    arc_data.append('NOT ON ARC')
    arc_data.append('NOT ON ARC')
    arc_data.append('NOT ON ARC')
    create_row(conn, arc_data)

    # dev data
    dev_data = []
    dev_main = post_dev_json_res['Main']
    dev_data.append('d')
    dev_data.append(post_dev_json_res['FCLTY_CD'])
    dev_data.append(post_dev_json_res['ITEM_CD'])
    dev_data.append(dev_main['ITEM_LONG_NM'])
    dev_data.append(dev_main['RTL_UNIT_SIZE_DS'])
    dev_data.append(dev_main['RTL_UNT_INR_CSE'])
    dev_data.append(dev_main['BSP_CRNT_AM'])
    dev_data.append(dev_main['ITEM_UPC_CD'])
    dev_data.append(dev_main['CASE_UPC_CD'])
    dev_data.append(dev_main['RTL_CITY_CRNT_GRS_PRFT_PC'])
    dev_data.append(dev_main['RTL_RRL_CRNT_GRS_PRFT_PC'])
    dev_data.append(dev_main['SELL_AT_WGT'])
    dev_data.append(dev_main['INVNTRY_DPRTMNT_CD'])
    dev_data.append(dev_main['ITEM_STATUS'])
    dev_data.append(dev_main['CTGRY_NB'])
    dev_data.append(dev_main['ITEM_STP_DT'])
    dev_data.append(dev_main['DSCNTND_DT'])
    dev_data.append(dev_main['BUY_NAM'])
    dev_data.append(dev_main['AWG_SBSTTTN_ITEM_CD'])
    dev_data.append(dev_main['WRHS_PALLET_QT'])
    dev_data.append(dev_main['LGCL_ORDR_QT'])
    dev_data.append(dev_main['CURRENT_INVENTORY'])
    dev_data.append(dev_main['GRP_LED_TM'])
    dev_data.append(dev_main['ITEM_MVNT'][0]['WK_START_DT'])
    dev_data.append(dev_main['ITEM_MVNT'][0]['WK_END_DT'])
    dev_data.append(dev_main['ITEM_MVNT'][0]['PAST_WKS_QTY_SHP'])
    dev_data.append(dev_main['ITEM_MVNT'][1]['WK_START_DT'])
    dev_data.append(dev_main['ITEM_MVNT'][1]['WK_END_DT'])
    dev_data.append(dev_main['ITEM_MVNT'][1]['PAST_WKS_QTY_SHP'])
    dev_data.append(dev_main['ITEM_MVNT'][2]['WK_START_DT'])
    dev_data.append(dev_main['ITEM_MVNT'][2]['WK_END_DT'])
    dev_data.append(dev_main['ITEM_MVNT'][2]['PAST_WKS_QTY_SHP'])
    create_row(conn, dev_data)

def select_all_arc_data(conn):
    """
    Return all records
    :param conn:
    """       
    cur = conn.cursor()
    cur.execute("SELECT * FROM arc_and_dev_data WHERE arc_or_dev IS 'a';")
    rows = cur.fetchall()
    return rows
    
def select_all_dev_data(conn):
    """
    Return all records
    :param conn:
    """       
    cur = conn.cursor()
    cur.execute("SELECT * FROM arc_and_dev_data WHERE arc_or_dev IS 'd';")
    rows = cur.fetchall()
    return rows

if __name__ == '__main__':
    create_db()

