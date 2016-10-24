from lxml import etree
import pandas as pd
import re

'''
ns = etree.FunctionNamespace("http://v8.1c.ru/data")
ns.prefix = "v8"
tree = etree.parse('./clients.xml')

data = []

nodes = tree.xpath('//CatalogObject.Контрагенты') # Открываем раздел

for node in nodes: # Перебираем элементы
    firm_code = node.xpath('./КодПоЕДРПОУ/text()')
    if len(firm_code) == 0:
        firm_code = ''
    else: firm_code = firm_code[0]

    firm_name = node.xpath('./Description/text()')
    if len(firm_name) == 0:
        firm_name = ''
    else: firm_name = firm_name[0]

    comment = node.xpath('./Комментарий/text()')
    if len(comment) == 0:
        comment = ''
    else: comment = comment[0]

    # print(firm_code)
    # print(firm_name)
    # print(comment)
    # print('\n')

    data.append((firm_code, firm_name, comment))


df = pd.DataFrame(data, columns=['Код', 'Назва', 'Комментар'])

writer = pd.ExcelWriter('clients_full.xlsx', engine='xlsxwriter')

df.to_excel(writer, sheet_name='Sheet_1', index=None)
writer.close()
'''

pat_words = re.compile('([\D\s\"\']+)', re.IGNORECASE)
pat_phone = re.compile(r'\s{1}(\d+)', re.IGNORECASE)

xl = pd.ExcelFile('./outer/final_load.xls')
df = xl.parse()
df.columns = ['Code', 'Data']

cols = ['Код', 'Название предприятия', 'Контактная особа', 'Номер телефон']
ndf = pd.DataFrame(columns=cols, dtype='object')

# print(df.head(5))

unic = dict()

for idx, row in df['Code'].iteritems():
    if row in unic:
        continue
    else:
        unic[row] = idx

for index in unic.values():

    row = df.loc[index]

    record = str(row['Code'])

    if len(record) < 8:
        record = '0' * abs(len(record)-8) + record
        if record == '00000000':
            continue
        else:
            firm_code = record
    else:
        firm_code = record

    raw_data = str(row['Data'])
    lst = re.search(pat_words, raw_data).group(0).split(',')
    if len(lst) < 2:
        firm_name = lst[0]
        person = ''
    else:
        firm_name = lst[0]
        person = lst[1]
    try:
        phone_number = re.search(pat_phone, raw_data).group(0)
    except AttributeError:
        phone_number = ''

    dic = {
        'Код': str(firm_code),
        'Название предприятия': str(firm_name),
        'Контактная особа': str(person),
        'Номер телефон': str(phone_number)
    }

    ndf = ndf.append(dic, ignore_index=True)


ndf.to_csv('./outer/processed.csv', sep=';', encoding='cp1251', index=False)
