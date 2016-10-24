__author__ = 'Acheron'

from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
import pandas as pd
import numpy as np
import re
import os
import full_import
from data_finder import process_all

class Vault:

    def __init__(self, host, port, db_name, user=None, password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.db = None

        self._connect()
        self._initialize_constants()

    def _initialize_constants(self):

        # patterns
        self._pat_index = re.compile(r'^(\d+)')
        self._pat_kved = re.compile(r'^([\d.]+)')

        self._cols_imp_low = ["Найменування", "Скорочена назва", "Код ЄДРПОУ", "Місцезнаходження", "ПІБ керівника",
                            "Основний вид діяльності", "Стан"]

        self._cols_exp_full = ['Дата додання', 'Найменування', 'Скорочена назва', 'Код ЄДРПОУ', 'Місцезнаходження',
                               'ПІБ керівника', 'Основний вид діяльності', 'Стан', 'Ключі ДФС', 'Ключі ДФС термін дії',
                               'Ключі Україна', 'Ключі Україна термін дії', 'Ліцензія Медок', 'Ліцензія Медок термін дії',
                               'Мітка_1', 'Керівник_дод.', 'Дата обрання на посаду', 'Засновник', 'Телефон']


    # Connecting to DB; Needs more parameters!!!
    def _connect(self):
        if not self.db:
            client = MongoClient('{0}:{1}'.format(self.host, self.port))
            self.db = client[self.db_name]

    # Filtering data from file and export to two pre-processed imp/upd files
    # Preparation stage.
    def prepare_import(self, file, region='Chernivtsi'):
        fields = self._cols_imp_low

        tmp_imp = pd.DataFrame(columns=fields, dtype='object')
        tmp_upd = pd.DataFrame(columns=fields, dtype='object')
        # df = self._read_dataframe(file)

        new_count = 0
        upd_count = 0
        ign_count = 0

        for item in full_import.from_file(file):
            idx, code, status, row = item

            # Pre-processing: normalizing firm code (adding zeros if code contains less than eight)
            record = code.strip()
            if len(record) < 8:
                record = '0' * abs(len(record)-8) + record
                if record == '00000000':
                    continue
                else:
                    code = record

            # Searching firm code in DB
            cur = self.db[region]['Firms'].find_one({'firm_code': code})

            # Firm code already in DB - performing status changes check
            if cur:
                if cur['state'] == status:
                    # No changes in firm status - just ignoring
                    if row['Найменування'] == cur['firm_name']:
                        ign_count += 1
                        continue
                    # Firm changed it's name; Saving it for further DB update
                    else:
                        tmp_upd = tmp_upd.append(row, ignore_index=True)
                        upd_count += 1
                # Firm changed it's status (eg: from 'Registered' to 'Closed'); Saving it for further DB update
                else:
                    tmp_upd = tmp_upd.append(row, ignore_index=True)
                    upd_count += 1
            # Firm code not in DB - new organization or stopped.
            else:
                # Newly registered firm; Saving for further insertion into DB
                if status == 'зареєстровано':
                    tmp_imp = tmp_imp.append(row, ignore_index=True)
                    new_count += 1
                else:
                    ign_count += 1

        tmp_imp.to_csv('./temp/tmp_imp.csv', sep=';', encoding='cp1251', index=False)
        tmp_upd.to_csv('./temp/tmp_upd.csv', sep=';', encoding='cp1251', index=False)

        # Returning firms counters (new, update, ignore)
        return (new_count, upd_count, ign_count)

    # Manual DB update from CSV file
    def prepare_update(self, file, region='Chernivtsi'):
        for item in full_import.from_file(file):
            idx, code, status, row = item

            cur = self.db['Chernivtsi']['Firms'].find_one({'firm_code': code})
            if cur:
                if status == 'припинено' and row['Найменування'] != cur['firm_name']:
                    # Сменили форму собственности, оставляем запись в БД
                    pass
                elif status == 'припинено' and row['Найменування'] == cur['firm_name']:
                    # Организация перестала существовать, обновляем запись (и ставим отметку на удаление?)
                    status = {
                        'exported': False,
                        'check': False,
                        'delete': True,
                        'dss': False
                    }
                    self.update_client(row, region, status)
                elif status == 'зареєстровано' and row['Найменування'] != cur['firm_name']:
                    # странный случай, когда предприятие просто меняет название; обновляем запись
                    self.update_client(row, region)
                else:
                    # предприятие меняет состояние регистрации; обновляем запись
                    self.update_client(row, region)
            else:
                # Новая организация, не понятно как сюда попала о__О; Добавляем запись в базу
                self.add_client(row, region)

    # Converts string date record for storing in DB

    # Important! Importing CSV file to DB
    def make_import(self, file, region='Chernivtsi'):
        self.prepare_import(file, region)
        print('Prepare done!')

        # Добавляет новые предприятия в БД
        tmp_file = './temp/tmp_imp.csv'
        if os.path.exists(tmp_file):
            imp_file = process_all(tmp_file)

            print('Firms before import: {0}'.format(self.records_count()))
            df = self._read_dataframe(imp_file)
            self.import_to_db(df)
            print('Firms after import: {0}'.format(self.records_count()))

         # Обновляет данные о предприятиях
        tmp_file_upd = './temp/tmp_upd.csv'
        if os.path.exists(tmp_file_upd):
            upd_file = process_all(tmp_file_upd)

            self.prepare_update(upd_file)

        # Shall return absolute path
        return (tmp_file, tmp_file_upd)

    # Update all additional info in records; Need to recheck!
    def update_data(self, file):
        cols = ['Найменування', 'Код ЄДРПОУ']
        out_frame = pd.DataFrame(columns=cols, dtype='object')

        cur = self.db['Chernivtsi']['Firms'].find({})
        for item in cur:
            if isinstance(item['additional']['phone'], float):
                data = {
                    'Найменування': item['firm_name'],
                    'Код ЄДРПОУ': item['firm_code'],
                    'Стан': item['state']
                }
                out_frame = out_frame.append(data, ignore_index=True)

        out_frame.to_csv(file, sep=';', encoding='cp1251', index=False)

        print('Dataframe saved')

        processed_file = process_all(file)

        print('Data processed')

        dataframe = pd.read_csv(processed_file, sep=';', encoding = "cp1251", low_memory=False, header=0, dtype='object')
        # тут вставить обновление строк в базе
        print(dataframe.head(5))

        for idx, record in dataframe["Код ЄДРПОУ"].iteritems():
            # sfs_keys = dataframe["sfs_keys"].loc[idx]
            # sfs_keys_date = dataframe["sfs_keys_date"].loc[idx]
            ua_keys = dataframe["ua_keys"].loc[idx]
            ua_keys_date = dataframe["ua_keys_date"].loc[idx]
            medoc_lic = dataframe["medoc_lic_type"].loc[idx]
            medoc_lic_date = dataframe["medoc_lic_date"].loc[idx]
            head_name = dataframe["head_name"].loc[idx]
            head_since = dataframe["head_since"].loc[idx]
            former = dataframe["former"].loc[idx]
            phone = dataframe["phone"].loc[idx]
            cur = self.db['Chernivtsi']['Firms'].update_one({'firm_code': record}, {'$set':
                                                                                        {
                                                                                            # 'details.sfs_keys': sfs_keys,
                                                                                            # 'details.sfs_date': sfs_keys_date,
                                                                                            'details.ua_keys': ua_keys,
                                                                                            'details.ua_keys_date': ua_keys_date,
                                                                                            'details.medoc_lic': medoc_lic,
                                                                                            'details.medoc_lic_date': medoc_lic_date,
                                                                                            'additional.head_name': head_name,
                                                                                            'additional.head_since': head_since,
                                                                                            'additional.former': former,
                                                                                            'additional.phone': phone
                                                                                        }})
        print('Done')

    # Inserting data to DB (creating documents); Need to recheck!
    def import_to_db(self, df, region='Chernivtsi'):
        positive = 0
        negative = 0

        for index, row in df.iterrows():
            # Check if firm definitely not exists in DB
            if not self.db[region]['Firms'].find_one({'firm_code': row['Код ЄДРПОУ']}):
                # Record creation timestamp
                dt = datetime.now()

                firm_index = re.search(self._pat_index, row['Місцезнаходження']).group()
                if row['Основний вид діяльності'] == None:
                    firm_main_kved = re.search(self._pat_kved, row['Основний вид діяльності']).group()
                else:
                    firm_main_kved = '0'
                default_date = self.date_convert('01/01/1970')
                ua_keys_date = self.date_convert(row['ua_keys_date'])
                medoc_lic_date = self.date_convert(row['medoc_lic_date'])
                # SFS keys missing!!!!
                # Not missing - it's default date; Need recheck

                print('Importing: {0}'.format(row['Код ЄДРПОУ']))

                firm_data = {
                    'timestamp': dt,
                    'firm_name': row['Найменування'],
                    'sfirm_name': row['Скорочена назва'],
                    'firm_code': row['Код ЄДРПОУ'],
                    'location': row['Місцезнаходження'],
                    'index': firm_index,
                    'head': row['ПІБ керівника'],
                    'activity_type': row['Основний вид діяльності'],
                    'main_kved': firm_main_kved,
                    'state': row['Стан'],
                    'details':
                        {
                            # 'sfs_keys': row['skeys'],
                            # 'sfs_keys_date': default_date,
                            'sfs_keys': '',
                            'sfs_keys_date': default_date,
                            'ua_keys': row['ua_keys'],
                            'ua_keys_date': ua_keys_date,
                            'medoc_lic': row['medoc_lic_type'],
                            'medoc_lic_date': medoc_lic_date
                        },
                    'status':
                        {
                            'exported': False,
                            'check': False,
                            'delete': False,
                            'dss': False
                        }
                }
                result = self.db[region]['Firms'].insert_one(firm_data)
                '''
                if not result:
                    raise ValueError('Error inserting record to DB.\nData: {0}'.format(firm_data))
                '''
                positive += 1
            else:
                negative += 1

        print('Import finished!')
        print('Successfully imported: {0}'.format(positive))
        print('Rejected (duplicates): {0}'.format(negative))
        print('Total processed: {0}'.format(positive+negative))
        return True

        # Inserting one record to DB

       # Export from DB to CSV file

    # Exports DB to CSV file
    def export_from_db(self, file, limit=0, region='Chernivtsi'):
        out_frame = self._create_dataframe(limit)
        out_frame.to_csv(file, sep=';', encoding='cp1251', index=False)

        return True

    # Add's record to DB (inserting document)
    def add_client(self, row, region, add_data=None):
        dt = datetime.now()
        firm_index = re.search(self._pat_index, row['Місцезнаходження']).group()
        if row['Основний вид діяльності'] == None:
            firm_main_kved = re.search(self._pat_kved, row['Основний вид діяльності']).group()
        else:
            firm_main_kved = '0'
        default_date = self.date_convert('01/01/1970')
        ua_keys_date = self.date_convert(row['ua_keys_date'])
        medoc_lic_date = self.date_convert(row['medoc_lic_date'])

        if add_data:
            status = {
                'exported': add_data['exported'],
                'check': add_data['check'],
                'delete': add_data['delete'],
                'dss': add_data['dss']
            }

            additional = {
                'head_name': add_data['head_name'],
                'head_since': add_data['head_since'],
                'former': add_data['former'],
                'phone': add_data['phone']
            }

        else:
            status = {
                'exported': False,
                'check': False,
                'delete': False,
                'dss': False
            }

            additional = {
                'head_name': '',
                'head_since': '',
                'former': '',
                'phone': ''
            }


        firm_data = {
            'timestamp': dt,
            'firm_name': row['Найменування'],
            'sfirm_name': row['Скорочена назва'],
            'firm_code': row['Код ЄДРПОУ'],
            'location': row['Місцезнаходження'],
            'index': firm_index,
            'head': row['ПІБ керівника'],
            'activity_type': row['Основний вид діяльності'],
            'main_kved': firm_main_kved,
            'state': row['Стан'],
            'details':
                {
                    # 'sfs_keys': row['skeys'],
                    # 'sfs_keys_date': default_date,
                    'sfs_keys': '',
                    'sfs_keys_date': default_date,
                    'ua_keys': row['ua_keys'],
                    'ua_keys_date': ua_keys_date,
                    'medoc_lic': row['medoc_lic_type'],
                    'medoc_lic_date': medoc_lic_date
                },
            'status': status,
            'additional': additional
        }
        result = self.db[region]['Firms'].insert_one(firm_data)
        return True

    # Updating(replacing) record in DB
    def update_client(self, row, region, add_data=None):
        dt = datetime.now()
        firm_index = re.search(self._pat_index, row['Місцезнаходження']).group()
        if row['Основний вид діяльності'] == None:
            firm_main_kved = re.search(self._pat_kved, row['Основний вид діяльності']).group()
        else:
            firm_main_kved = '0'

        default_date = self.date_convert('01/01/1970')
        try:
            ua_keys_date = self.date_convert(row['ua_keys_date'])
            medoc_lic_date = self.date_convert(row['medoc_lic_date'])
        except:
            print('Error on {0}'.format(row['Код ЄДРПОУ']))

        if add_data:
            status = {
                'exported': add_data['exported'],
                'check': add_data['check'],
                'delete': add_data['delete'],
                'dss': add_data['dss']
            }

            additional = {
                'head_name': add_data['head_name'],
                'head_since': add_data['head_since'],
                'former': add_data['former'],
                'phone': add_data['phone']
            }
        else:
            status = {
                'exported': False,
                'check': False,
                'delete': False,
                'dss': False
            }

            additional = {
                'head_name': '',
                'head_since': '',
                'former': '',
                'phone': ''
            }


        firm_data = {
            'timestamp': dt,
            'firm_name': row['Найменування'],
            'sfirm_name': row['Скорочена назва'],
            'firm_code': row['Код ЄДРПОУ'],
            'location': row['Місцезнаходження'],
            'index': firm_index,
            'head': row['ПІБ керівника'],
            'activity_type': row['Основний вид діяльності'],
            'main_kved': firm_main_kved,
            'state': row['Стан'],
            'details':
                {
                    # 'sfs_keys': row['skeys'],
                    # 'sfs_keys_date': default_date,
                    'sfs_keys': '',
                    'sfs_keys_date': default_date,
                    'ua_keys': row['ua_keys'],
                    'ua_keys_date': ua_keys_date,
                    'medoc_lic': row['medoc_lic_type'],
                    'medoc_lic_date': medoc_lic_date
                },
            'status': status,
            'additional': additional
        }

        result = self.db[region]['Firms'].replace_one({'firm_code':row['Код ЄДРПОУ']}, firm_data)
        return result

    # Date format - day(2)/month(2)/year(4)
    def date_convert(self, string):
        if not isinstance(string, str):
            string = '01/01/1970'
        day, month, year = string.split('/')
        return datetime(int(year), int(month), int(day), 0, 0, 0, 0)

    # Создает словарь для формирования на его основе datafram'а
    def _make_dict_for_export(self, dic):
        dic = {
            'Дата додання': dic['timestamp'],
            'Найменування': dic['firm_name'],
            'Скорочена назва': dic['sfirm_name'],
            'Код ЄДРПОУ': dic['firm_code'],
            'Місцезнаходження': dic['location'],
            'ПІБ керівника': dic['head'],
            'Основний вид діяльності': dic['activity_type'],
            'Стан': dic['state'],
            'Ключі ДФС': dic['details']['sfs_keys'],
            'Ключі ДФС термін дії': dic['details']['sfs_keys_date'],
            'Ключі Україна': dic['details']['ua_keys'],
            'Ключі Україна термін дії': dic['details']['ua_keys_date'],
            'Ліцензія Медок': dic['details']['medoc_lic'],
            'Ліцензія Медок термін дії': dic['details']['medoc_lic_date'],
            'Мітка_1': dic['status']['dss'],
            'Керівник_дод.': dic['additional']['head_name'],
            'Дата обрання на посаду': dic['additional']['head_since'],
            'Засновник': dic['additional']['former'],
            'Телефон': dic['additional']['phone']
        }

        return dic

    # Returns records in DB
    def records_count(self, region='Chernivtsi'):
        return self.db[region].Firms.count()

    # Make dataframe from CSV file; Return dataframe
    def _read_dataframe(self, file):
        df = pd.read_csv(file, sep=';', encoding = "cp1251", low_memory=False, header=0, dtype='object')
        return df

    # Creates dataframe based on DB records
    def _create_dataframe(self, limit=0, region='Chernivtsi'):
        # cols = ['Дата додання', 'Найменування', 'Скорочена назва', 'Код ЄДРПОУ', 'Місцезнаходження', 'ПІБ керівника',
        #         'Основний вид діяльності', 'Стан', 'Ключі ДФС', 'Ключі ДФС термін дії', 'Ключі Україна',
        #         'Ключі Україна термін дії', 'Ліцензія Медок', 'Ліцензія Медок термін дії', 'Мітка_1']

        out_frame = pd.DataFrame(columns=self._cols_exp_full, dtype='object')

        counter = 0
        cur = self.db['Chernivtsi']['Firms'].find({'status.delete': False})
        for item in cur:
            limit += 1
            if counter > limit:
                break
            row = self._make_dict_for_export(item)
            out_frame = out_frame.append(row, ignore_index=True)

        return out_frame

    def testing_p(self):
        xls = pd.ExcelFile('./outer/final_load.xls')
        dt_imp = xls.parse()
        dt_imp.columns = ['Code', 'Data']

        pos = 0
        neg = 0

        for idx, record in dt_imp['Code'].iteritems():
            record = str(record).strip()

            cur = self.db['Chernivtsi']['Firms'].find_one({'firm_code': record})
            if cur:
                pos += 1
                self.db['Chernivtsi']['Firms'].update_one({'firm_code': record}, {'$set' : {"status.dss": True}})
            else:
                if len(record) == 8:
                    neg += 1
        print(pos)
        print(neg)

        # res = self.db['Chernivtsi']['Firms'].update_many({}, {'$set' : {"status.dss": False}}, False)
        # res = self.db['Chernivtsi']['Firms'].find_one({'DSS': False})
        # self.db['Chernivtsi']['Firms'].remove(ObjectId(res['_id']))
        # for cur in self.db['Chernivtsi']['Firms'].find({'status.dss': False}).count():
        #     print(cur)

    def addField(self, name, value=False):
        dic = {
                'head_name': '',
                'head_since': '',
                'former': '',
                'phone': ''
        }
        # res = self.db['Chernivtsi']['Firms'].update_many({}, {'$set' : {name: value}}, False)
        # res = self.db['Chernivtsi']['Firms'].update_many({ "additional" : { "$exists" : False }}, {'$set' : {'additional': dic}}, False)
        # print(res)
        # cur = self.db['Chernivtsi']['Firms'].find_one({})
        # print(cur)

        cur = self.db['Chernivtsi']['Firms'].find({ "additional" : { "$exists" : False }})
        counter = 0
        for item in cur:
            print(item)
            # if isinstance(item['additional']['phone'], float):
            #     print('{0}: {1}'.format(item['additional']['phone'], type(item['additional']['phone'])))
            counter += 1

        print(counter)


class Vault_Frontend:

    def __init__(self, db_obj, region):
        self.__ref = db_obj
        self.db = db_obj.db
        self.region = region

    def build_data(self):
        df = self.__ref._create_dataframe()
        data = np.array(df.values)
        return data

    def records_count(self):
        return self.db[self.region].Firms.count()

    def columns_count(self):
        return len(self.get_columns())

    def get_columns_en(self):
        nested = {'status', 'details', 'additional'}
        restricted = {'index', '_id'}

        columns = []

        probe = self.db[self.region].Firms.find_one()

        for col in probe.keys():
            if col in restricted:
                pass
            elif col in nested:
                for element in probe[col].keys():
                    columns.append(element)
            else:
                columns.append(col)

        return sorted(columns)

    def get_columns_ua(self):
        return self.__ref._cols_exp_full





class Qt_Model_View_Interface:

    def __init__(self, db_obj):
        self.__ref = db_obj
        self.db = db_obj.db

        # Organization binded columns headers list
        self.org_headers_en = ['timestamp', 'firm_name', 'sfirm_name', 'firm_code', 'index', 'location', 'head',
                               'activity_type', 'main_kved', 'state', 'sfs_keys', 'sfs_keys_date', 'ua_keys',
                               'ua_keys_date', 'medoc_lic', 'medoc_lic_date', 'exported', 'check', 'delete']

        self.org_headers_ukr = ['Додано до БД', 'Найменування', 'Скорочена назва', 'Код ЄДРПОУ', 'Поштовий індекс',
                            'Місцезнаходження', 'ПІБ керівника', 'Основний вид діяльності', 'Квед',
                            'Стан', 'Ключі ДФС', 'Ключі ДФС термін дії', 'Ключі Україна',
                            'Ключі Україна термін дії', 'Ліцензія Медок', 'Ліцензія Медок термін дії',
                            'Єкспортовано', 'Перевірити', 'На видалення']

        self.nested_header_detail = ['sfs_keys', 'sfs_keys_date', 'ua_keys', 'ua_keys_date',
                                     'medoc_lic', 'medoc_lic_date']

        self.nested_header_status = ['exported', 'check', 'delete']

    def records_count(self, region='Chernivtsi'):
        return self.db[region].Firms.count()

    def get_headers(self, region='Chernivtsi'):
        restricted = ['_id', 'state', 'details']
        cur = self.db[region]['Firms'].find_one()
        detail = [key for key in cur['details'].keys()]
        status = [key for key in cur['status'].keys()]
        return [key for key in cur.keys() if key not in restricted] + detail + status

    # Binded specially for Qt Table
    def get_headers_binded_ukr(self, region='Chernivtsi'):
        return self.org_headers_ukr

    def get_headers_binded_en(self, region='Chernivtsi'):
        return ['timestamp', 'firm_name', 'sfirm_name', 'firm_code', 'index', 'location', 'head',
                               'activity_type', 'main_kved', 'state', 'sfs_keys', 'sfs_keys_date', 'ua_keys',
                               'ua_keys_date', 'medoc_lic', 'medoc_lic_date', 'exported', 'check', 'delete']

    def get_cursor(self, region='Chernivtsi'):
        return self.db[region]['Firms'].find()

    def get_item(self, index, key, region='Chernivtsi'):
        if index <= self.records_count():
            record = self.db[region]['Firms'].find()[index]
            if key in self.nested_header_detail:
                return record['details'][key]
            elif key in self.nested_header_status:
                return record['status'][key]
            else:
                return record[key]

    def get_item_alt(self, index_map, index, key, region='Chernivtsi'):
        if index <= self.records_count():
            record = index_map[index]
            if key in self.nested_header_detail:
                return record['details'][key]
            elif key in self.nested_header_status:
                return record['status'][key]
            else:
                return record[key]

    def find_by_id(self, id, key, region='Chernivtsi'):
        record = self.db[region]['Firms'].find_one({'_id': ObjectId(id)})
        if key in self.nested_header_detail:
            return record['details'][key]
        elif key in self.nested_header_status:
            return record['status'][key]
        else:
            return record[key]

    def build_indexes(self):
        cur = self.get_cursor()
        indexes = dict()
        for i in range(cur.count()):
            indexes[i] = cur[i]['_id']
        return indexes

    def build_indexes_alt(self):
        cur = self.get_cursor()
        indexes = dict()
        for i in range(cur.count()):
            indexes[i] = cur[i]
        return indexes

    def build_indexes_alt2(self):
        cur = self.get_cursor()
        indexes = []
        for record in cur:
            values = []
            for value in record.values():
                if isinstance(value, dict):
                    for sub_value in value:
                        values.append(sub_value)
                else:
                    values.append(value)
            indexes.append(values)
        return indexes

a = Vault('localhost', '27017', db_name='Clients')
# a.make_import('./import/UO.csv')
# a.prepare_update('./temp/tmp_imp_stage_3.csv')
# a.export_from_db('./out2.csv')

# print(a.org_headers)
# b = Qt_Model_View_Interface(a)
# indexes = b.build_indexes_alt2()
# print(indexes[0])
# print(a.get_headers(True))

# a.testing_p()
# a.addField('status.dss')
# a.update_data('./temp/update.csv')
# a.addField(1)
# b = Vault_Frontend(a, 'Chernivtsi')
# x = b.build_data()
