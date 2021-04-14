# Модуль для взаимодействия с БД и получения информации о
# списке таблиц, их полях и данных из таблиц

from connect_to_atirra import db_connect

# Функция получения таблиц в БД
def get_tables() -> list:
    tables_list = []
    # Соединение с БД
    try:
        cur, con = db_connect()
    except Exception as e:
        print(f'Не удалось подключиться к базе данных. {e}')
    try:    
        query = "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND RDB$VIEW_BLR IS NULL ORDER BY RDB$RELATION_NAME"
        result = cur.execute(query)
        print("tables list")
        for i in result:
            i = str(i).split(' ')[0]
            i = i.split('(\'')[1]
            tables_list.append(i)
            print(i)
        return tables_list
    except Exception as e:
        print(f'Не удалось получить список таблиц из БД. {e}')

# Функция получение списка полей в таблице
def get_table_fields(tables_list:list) -> list:
    # Список для хранения полей таблиц
    tables = {}
    fields_list = []
    table_id = 0
    # Соединение с БД
    try:
        cur, con = db_connect()
    except Exception as e:
        print(f'Не удалось подключиться к базе данных. {e}')
        
    # Получение списка полей таблиц
    try:
        for table in tables_list:
            query = f"select RDB$FIELD_NAME from rdb$relation_fields where RDB$RELATION_NAME = '{table}'"
            result = cur.execute(query)
            for field in result:
                field = str(field).split(' ')[0]
                field = field.split('(\'')[1]
                fields_list.append(field)
            tables[table]=fields_list
            fields_list = []
            print(f"table {table} fields")
            print(tables[table])
            table_id += 1
        #return fields_list
    except Exception as e:
        print(f'Не удалось получить список полей в таблице {table}. {e}')

# Получение интересующих данных из таблиц
def get_table_data(tables:list):
    # Соединение с БД
    try:
        cur, con = db_connect()
    except Exception as e:
        print(f'Не удалось подключиться к базе данных. {e}')
        
    # Получение данных из таблицы
    try:
        for table in tables:
            print(f"data from {table}")
            customer_data_query = cur.execute(f"SELECT * FROM {table} WHERE ACCOUNT_NO = '124'")
            query_result = customer_data_query.fetchall()
            for i in query_result:
                print(i)
    except Exception as e:
        print('Не удалось получить данные из таблицы {table}. {e}')

tables = ['CUSTOMER']
get_table_fields(tables)
get_table_data(tables)
