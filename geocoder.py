import sqlite3
import argparse
from preprocessing import parser, load_database, load_xml

DB = "geocoder.db"


def get_regions(db, limit=20):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("SELECT short_name, region_name FROM regions "
                   "ORDER BY region_name LIMIT ?", (limit, ))
    res = []
    for row in cursor.fetchall():
        res.append(f"{row[1]} {row[0]}")

    conn.close()

    if len(res) == 0:
        return "В базе данных пока нет регионов"
    return "\n".join(res)


def get_areas(db, limit=20):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("SELECT short_name, area_name FROM areas "
                   "ORDER BY area_name LIMIT ?", (limit, ))
    res = []
    for row in cursor.fetchall():
        res.append(f"{row[0]} {row[1]}")

    conn.close()

    if len(res) == 0:
        return "В базе данных пока нет районов"
    return "\n".join(res)


def get_streets(db, limit=20):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("SELECT short_name, street_name FROM streets "
                   "WHERE short_name != 'мкр'"
                   "ORDER BY street_name LIMIT ?", (limit, ))
    res = []
    for row in cursor.fetchall():
        res.append(f"{row[0]}. {row[1]}")

    conn.close()

    if len(res) == 0:
        return "В базе данных пока нет улиц"
    return "\n".join(res)


def get_cities(db, limit=20):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("SELECT short_name, city_name FROM cities "
                   "ORDER BY city_name LIMIT ?", (limit, ))
    res = []
    for row in cursor.fetchall():
        res.append(f"{row[0]}. {row[1]}")

    conn.close()

    if len(res) == 0:
        return "В базе данных пока нет городов"
    return "\n".join(res)


def find_formal_address(db, address):
    addr = address.split(";")
    result = "Адреса '{}' нет в базе".format(" ".join(addr))
    if len(addr) not in [3, 4]:
        return result

    sql_query = "SELECT regions.region_id, region_name, regions.short_name," \
                " areas.short_name, area_name," \
                " cities.short_name, city_name," \
                " streets.short_name, street_name," \
                " house_number FROM regions" \
                " INNER JOIN cities ON regions.region_id = cities.region_id" \
                " LEFT JOIN areas ON cities.parent_ao_guid = area_id" \
                " INNER JOIN streets ON streets.parent_ao_guid = city_id" \
                " INNER JOIN houses ON houses.parent_ao_guid = street_id" \
                f" WHERE region_name = '{addr[0]}'" \
                f" AND city_name = '{addr[1]}'" \
                f" AND street_name = '{addr[2]}';"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    if len(addr) == 4:
        sql_query = sql_query[:len(sql_query) - 1] + f" AND house_number " \
                                                     f"= {addr[3]};"

    cursor.execute(sql_query)
    db_addrs = cursor.fetchall()
    if not len(db_addrs):
        return result
    res_list = []

    if db_addrs[0][3]:
        for db_addr in db_addrs:
            res_list.append(f"{db_addr[1]} {db_addr[2]}, {db_addr[3]}. "
                            f"{db_addr[4]}, {db_addr[5]}. {db_addr[6]}, "
                            f"{db_addr[7]}. {db_addr[8]}, д. {db_addr[9]}")
    else:
        for db_addr in db_addrs:
            res_list.append(f"{db_addr[1]} {db_addr[2]}, {db_addr[5]}. "
                            f"{db_addr[6]}, {db_addr[7]}. {db_addr[8]} "
                            f"д. {db_addr[9]}")
    conn.close()

    return "\n".join(res_list)


def search(db, addr):
    addr = addr.split(";")
    result = "Адреса '{}' нет в базе".format(" ".join(addr))
    if len(addr) not in [3, 4]:
        return result

    conn = sqlite3.connect(db)
    result = get_formal_address(conn.cursor(), addr)
    conn.close()

    return result


def get_formal_address(cursor, addr):
    region_area_city = get_region_area_city(cursor, addr[0], addr[1])
    if not region_area_city:
        return None

    street = get_street(cursor, region_area_city[7], addr[2])
    if not street:
        return None

    house = get_house(cursor, street[2], addr[3])
    if not house:
        return None

    if region_area_city[3]:
        return f"{region_area_city[1]} {region_area_city[2]}, " \
               f"{region_area_city[3]}. {region_area_city[4]}, " \
               f"{region_area_city[5]}. {region_area_city[6]}, " \
               f"{street[0]}. {street[1]}, д. {house}"
    else:
        return f"{region_area_city[1]} {region_area_city[2]}, " \
               f"{region_area_city[5]}. {region_area_city[6]}, " \
               f"{street[0]}. {street[1]}, д. {house}"


def get_region_area_city(cursor, region, city):
    sql_query = "SELECT regions.region_id, region_name, regions.short_name," \
                " areas.short_name, area_name," \
                " cities.short_name, city_name, city_id" \
                " FROM regions" \
                " INNER JOIN cities ON regions.region_id = cities.region_id" \
                " LEFT JOIN areas ON cities.parent_ao_guid = area_id" \
                f" WHERE region_name = '{region}'" \
                f" AND city_name = '{city}';"
    cursor.execute(sql_query)
    db_addrs = cursor.fetchall()
    if not len(db_addrs):
        return None
    return db_addrs[0]


def get_street(cursor, city_ao_guid, street):
    sql_query = "SELECT streets.short_name, street_name, street_id" \
                " FROM streets" \
                f" WHERE street_name = '{street}'" \
                f" AND streets.parent_ao_guid = '{city_ao_guid}';"
    cursor.execute(sql_query)
    db_addrs = cursor.fetchall()
    if not len(db_addrs):
        return None
    return db_addrs[0]


def get_house(cursor, street_ao_guid, house=None):
    sql_query = "SELECT house_number FROM houses" \
                f" WHERE houses.parent_ao_guid = " \
                f"'{street_ao_guid}';"
    if house:
        sql_query = sql_query[:len(sql_query) - 1] + f" AND house_number " \
                                                              f"= {house};"
    cursor.execute(sql_query)
    db_addrs = cursor.fetchall()
    if not len(db_addrs):
        return None
    return db_addrs[0][0]


def main():
    argparser = argparse.ArgumentParser()
    proc_arg_group = argparser.add_mutually_exclusive_group()
    proc_arg_group.add_argument('-cxml', '--convert_xml_db', nargs=3, type=str,
                                dest='convert_xml_db', required=False,
                                metavar=('db_file', 'ao_file', 'house_file'),
                                help='Преобразование XML-файлов БД ФИАС '
                                     '[ao_file] и [house_file] в '
                                     'базу данных [db_file]')
    proc_arg_group.add_argument('-lxml', '--load_xml', action='store_true',
                                dest='load_xml', required=False,
                                help='Скачивание XML-файлов БД ФИАС '
                                     'AS_ADDROBJ и AS_HOUSE')
    proc_arg_group.add_argument('-ldb', '--load_database', nargs=1, type=str,
                                metavar='db_file', dest='load_database',
                                required=False, help='Скачивание с диска базы '
                                                     'данных в файл [db_file]')
    a_group = argparser.add_mutually_exclusive_group()
    a_group.add_argument('formal_address', type=str, action='store', nargs='?',
                         help='Полный адрес в формате '
                              '"регион;населённый пункт;улица;номер дома"'
                              ' или "регион;населённый пункт;улица"')
    argparser.add_argument('db_file', type=str, action='store', nargs='?',
                           default='geocoder_ekb.db', help='База данных')
    args = argparser.parse_args()

    if args.formal_address:
        print(search(args.db_file, args.formal_address))
    elif args.convert_xml_db:
        parser.convert_xml_db(args.convert_xml_db[1], args.convert_xml_db[2],
                              args.convert_xml_db[0])
    elif args.load_xml:
        load_xml.load_xml()
    elif args.load_database:
        load_database.load_database_from_disk(args.load_database[0])
    else:
        print('Такой команды нет\n'
              'Пожалуйста, воспользуйтесь справкой -h | --help')


if __name__ == '__main__':
    main()
