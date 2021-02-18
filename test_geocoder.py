import unittest
import sqlite3
import os
from preprocessing import parser, load_xml, load_database
import geocoder

TEST_URL_DB = 'https://yadi.sk/d/eOIPbWxDCI67Hg'


class GeocoderTests(unittest.TestCase):
    db_file = "test.db"

    if os.path.isfile(db_file):
        os.remove(db_file)
    parser.convert_xml_db("preprocessing/test_as_addrobj.xml",
                          "preprocessing/test_as_house.xml", db_file)

    def test_get_regions(self):
        self.assertEqual(geocoder.get_regions(self.db_file),
                         "Чукотский AO")

    def test_get_areas(self):
        self.assertEqual(geocoder.get_areas(self.db_file),
                         "р-н Иультинский")

    def test_get_cities(self):
        self.assertEqual(geocoder.get_cities(self.db_file),
                         "г. Анадырь\nп. Полярный")

    def test_get_streets(self):
        self.assertEqual(geocoder.get_streets(self.db_file, 3),
                         "ул. Есенина\nул. Комарова\nул. Ленина")

    def test_find_formal_address(self):
        self.assertEqual(
            geocoder.find_formal_address(self.db_file,
                                         "Чукотский;Анадырь;Чкалова;16"),
            "Чукотский AO, г. Анадырь, ул. Чкалова д. 16")

    def test_find_formal_address_with_areas(self):
        self.assertEqual(
            geocoder.find_formal_address(self.db_file,
                                         "Чукотский;Полярный;Ленина;32"),
            "Чукотский AO, р-н. Иультинский, п. Полярный, ул. Ленина, д. 32")

    def test_find_formal_address_fail(self):
        self.assertEqual(
            geocoder.find_formal_address(self.db_file,
                                         "Чукотский;Анадырь;Тургенева;4"),
            "Адреса 'Чукотский Анадырь Тургенева 4' нет в базе")
        self.assertEqual(
            geocoder.find_formal_address(self.db_file,
                                         "Ещё один некорректный адрес"),
            "Адреса 'Ещё один некорректный адрес' нет в базе")

    def test_find_formal_address_without_house(self):
        self.assertEqual(
            geocoder.find_formal_address(self.db_file,
                                         "Чукотский;Анадырь;Чкалова"),
            "Чукотский AO, г. Анадырь, ул. Чкалова д. 10\n"
            "Чукотский AO, г. Анадырь, ул. Чкалова д. 16\n"
            "Чукотский AO, г. Анадырь, ул. Чкалова д. 9")
        self.assertEqual(
            geocoder.find_formal_address(self.db_file,
                                         "Чукотский;Полярный;Ленина"),
            "Чукотский AO, р-н. Иультинский, п. Полярный, ул. Ленина, д. 32\n"
            "Чукотский AO, р-н. Иультинский, п. Полярный, ул. Ленина, д. 8")

    def test_extract_zip(self):
        load_xml.extract_zip('test.zip')
        self.assertIn('AS_ADDROBJ-test.XML', os.listdir())
        self.assertIn('AS_HOUSE-test.XML', os.listdir())
        os.remove('AS_ADDROBJ-test.XML')
        os.remove('AS_HOUSE-test.XML')

    def test_load_database_from_disk(self):
        load_database.load_database_from_disk('test_from_disk.db', TEST_URL_DB)
        self.assertIn('test_from_disk.db', os.listdir())
        os.remove('test_from_disk.db')

    def test_empty_table(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        for table in ['regions', 'areas', 'cities']:
            cursor.execute(f'DELETE FROM {table};')
        conn.commit()
        self.assertEqual(geocoder.get_regions(self.db_file),
                         "В базе данных пока нет регионов")
        self.assertEqual(geocoder.get_areas(self.db_file),
                         "В базе данных пока нет районов")
        self.assertEqual(geocoder.get_cities(self.db_file),
                         "В базе данных пока нет городов")
        conn.close()
        parser.convert_xml_db("preprocessing/test_as_addrobj.xml",
                              "preprocessing/test_as_house.xml", self.db_file)
