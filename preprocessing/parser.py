import xml.etree.cElementTree as Et
import sqlite3
import datetime as dt


ADDR_TABLE = '../AS_ADDROBJ_20200528_c5aed5f7-dd75-420d-8464-41ccac4d7b6d.XML'
HOUSE_TABLE = '../AS_HOUSE_20200528_41ca5f62-025e-452c-b4fe-8725e081b2dd.XML'


def interactive_loading(func):
    def wrapper(*args, **kwargs):
        print("Пожалуйста, подождите. Данные загружаются...")
        func(*args, **kwargs)
        print("Готово! Данные загружены")
    return wrapper


@interactive_loading
def convert_xml_db(ao_file, house_file, db_file):
    """Преобразует XML-файлы БД ФИАС AS_ADDROBJ и AS_HOUSE в sql-таблицы"""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    create_tables(db_file)
    parse_xml(ao_file, cursor, conn)
    conn.commit()
    parse_xml(house_file, cursor, conn)
    conn.commit()
    conn.close()


def create_tables(db_file):
    """Создаёт таблицы 'houses', 'streets', 'cities', 'areas', 'regions'
    и индексы"""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS regions ('
                   'region_id integer PRIMARY KEY,'
                   'region_name text,'
                   'short_name text)')

    cursor.execute('CREATE TABLE IF NOT EXISTS areas ('
                   'area_id text PRIMARY KEY,'
                   'area_name text,'
                   'short_name text,'
                   'parent_ao_guid text,'
                   'region_id integer)')

    cursor.execute('CREATE TABLE IF NOT EXISTS cities ('
                   'city_id text PRIMARY KEY,'
                   'city_name text,'
                   'short_name text,'
                   'parent_ao_guid text,'
                   'region_id integer)')

    cursor.execute('CREATE TABLE IF NOT EXISTS streets ('
                   'street_id text PRIMARY KEY,'
                   'street_name text,'
                   'short_name text,'
                   'parent_ao_guid text,'
                   'region_id integer)')

    cursor.execute('CREATE TABLE IF NOT EXISTS houses ('
                   'house_id text PRIMARY KEY,'
                   'house_number text,'
                   'build_number integer,'
                   'struc_number text, '
                   'parent_ao_guid text,'
                   'region_id integer)')

    cursor.execute('CREATE INDEX IF NOT EXISTS "area_name_index" ON "areas" '
                   '("area_name" ASC)')

    cursor.execute('CREATE INDEX IF NOT EXISTS "city_name_index" ON "cities" '
                   '("city_name" ASC)')

    cursor.execute('CREATE INDEX IF NOT EXISTS "street_name_index" '
                   'ON "streets" ("street_name" ASC)')

    cursor.execute('CREATE INDEX IF NOT EXISTS "house_number_index" '
                   'ON "houses" ("house_number" ASC)')


def parse_xml(xml_file, cursor, conn):
    """ Парсинг XML """
    tree_iter = Et.iterparse(xml_file)
    root = next(Et.iterparse(xml_file, events=['start']))[1].tag
    dt_now = dt.datetime.now()
    counter = 0

    def clear_iter(tree, process_func):
        for event, child in tree:
            try:
                process_func(child)
            except Exception as e:
                print(f"Ошибка при обработке записи:\n{child[1].attrib}\n{e}")

            nonlocal counter
            if counter > 100000:
                conn.commit()
                counter = 0
            child.clear()
        del tree

    def process_addr_objects(child):
        if len(child.attrib) > 0 and 'CURRSTATUS' in child.attrib and \
                child.attrib['CURRSTATUS'] == '0':
            insert_addr_object(child.attrib, cursor)
            nonlocal counter
            counter += 1

    def process_house_objects(child):
        if (len(child.attrib) > 0
                and child.attrib['ENDDATE'] >
                f'{dt_now.year}-{dt_now.month}-{dt_now.day}'):
            insert_house(child.attrib, cursor)
            nonlocal counter
            counter += 1

    if root == 'AddressObjects':
        clear_iter(tree_iter, process_addr_objects)
    elif root == 'Houses':
        clear_iter(tree_iter, process_house_objects)


def insert_addr_object(addr_obj, cursor):
    ao_level = int(addr_obj['AOLEVEL'])
    if ao_level == 1:
        cursor.execute("INSERT OR IGNORE INTO regions "
                       "(region_id, region_name, short_name) "
                       "VALUES (?, ?, ?) ", (addr_obj['REGIONCODE'],
                                             addr_obj['FORMALNAME'],
                                             addr_obj['SHORTNAME']))

    else:
        addr = (addr_obj['AOGUID'], addr_obj['FORMALNAME'],
                addr_obj['SHORTNAME'], addr_obj['PARENTGUID'],
                addr_obj['REGIONCODE'])

        if ao_level == 3:
            cursor.execute("INSERT OR IGNORE INTO areas "
                           "(area_id, area_name, short_name, "
                           "parent_ao_guid, region_id) "
                           "VALUES (?, ?, ?, ?, ?) ", addr)
        elif ao_level == 4 or ao_level == 6:
            cursor.execute("INSERT OR IGNORE INTO cities "
                           "(city_id, city_name, short_name, "
                           "parent_ao_guid, region_id) "
                           "VALUES (?, ?, ?, ?, ?) ", addr)
        elif ao_level == 7:
            cursor.execute("INSERT OR IGNORE INTO streets"
                           "(street_id, street_name, short_name, "
                           "parent_ao_guid, region_id) "
                           "VALUES (?, ?, ?, ?, ?) ", addr)


def insert_house(house_obj, cursor):
    if 'HOUSENUM' in house_obj:
        cursor.execute("INSERT OR IGNORE INTO houses "
                       "(house_id, house_number, "
                       "parent_ao_guid, region_id) "
                       "VALUES (?, ?, ?, ?) ",
                       (house_obj['HOUSEGUID'], house_obj['HOUSENUM'],
                        house_obj['AOGUID'], house_obj['REGIONCODE']))

    elif 'BUILDNUM' in house_obj:
        cursor.execute("UPDATE houses SET build_number = '{0}' "
                       "WHERE house_id = '{1}'".format(house_obj['BUILDNUM'],
                                                       house_obj['HOUSEGUID']))

    elif 'STRUCNUM' in house_obj:
        cursor.execute("UPDATE houses SET struc_number = '{0}' "
                       "WHERE house_id = '{1}'".format(house_obj['STRUCNUM'],
                                                       house_obj['HOUSEGUID']))
