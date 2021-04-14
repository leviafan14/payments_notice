# Содежит функции для получения данных об абоненте и для повторного подключения его услуги
import fdb
from datetime import datetime

# Функция создания подключения к базе данных
def db_connect():
    try:
        # Соединение с сервером и БД
        connection = fdb.connect(dsn='127.0.0.1:D:\ATIRRA\DB\ATIRRA_DB.fdb', user='SYSDBA', password='masterkey')
        # Создание объекта курсора
        cursor = connection.cursor()
        return cursor, connection
    except Exception as e:
        print(f'Не удалось подключиться к базе данных. {e}')
        return 0

# Функция получения статуса и описания абонента
def get_customer_data(abonent:int, update_status='Disable') -> str:
    global customer_id
    global adress_customer
    global state_service
    global description_service
    # Соединение с БД
    try:
        cur, con = db_connect()
    except Exception as e:
        print(f'Не удалось подключиться к базе данных. {e}')
        return 0
        
    # Шаблон, по которому скрипт определяет тип отключения
    descr_pattern = 'Общ.. Отключение неуплата'
   
    # Запрос на получение адреса, состояния и описания, услуги абонента
    customer_data_query = cur.execute(f"SELECT CUSTOMER_ID, CUST_CODE, CUST_STATE, CUST_STATE_DESCR FROM CUSTOMER WHERE ACCOUNT_NO='{abonent}'")
    query_result = str(customer_data_query.fetchone())
    
    # Распаковка результатов запроса
    try:
        customer_id, adress_customer, state_service, description_service = query_result.split(',')
        customer_id = query_result.split(',')[0]
        adress_customer = query_result.split(',')[1]
        state_service = query_result.split(',')[2]
        description_service = query_result.split(',')[3]
        # Отсечение лишних символов из индентификатора абонента
        customer_id = str(customer_id).split('(')[1]
        # Отсечение лишних символов из адреса абонента
        adress_customer = adress_customer.split('\'')[1]
        # Отсечение лишних символов из описания услуги 
        description_service = str(description_service.split('(')[0])
        description_service = str(description_service.split('\'')[1])
        print(customer_id, adress_customer, state_service, description_service)
    except Exception as e:
        print(f'Не удалось распаковать результат запроса данных пользователя. {e}')
        return str(abonent)
    # Определение статуса услуги абонента и причины отключения
    try:    
        if  len(description_service) > 0 and int(state_service) == 0 and descr_pattern == description_service:
            print('Отключение за неуплату')
            
         # Проверка, нужно ли вызывать функцию изменения статуса абонента
            if update_status == 'Enable':
                print('Автоматическое подключение пользователя')
                update_service_customer(abonent)
            else:
                print('Пользователя не нужно подключать вручную')
            
        else:
            print('Пользователь подключен, либо отключен по другой причине')

        return adress_customer
    
    except Exception as e:
        print(f' Ошибка в получении данныых {e}')
               
# Функция обновления статуса абонента
def update_service_customer(abonent:int) -> int:
    # Получение текущей даты для вставки в таблицы
    current_date = str(datetime.today().strftime("%d.%m.%Y"))
    # Текущая дата использующая для вывода на экран в поле статуса услуги абонента
    current_date_slash = str(datetime.today().strftime("%d/%m/%Y"))
    print(current_date_slash)
    # Переменная для хранения ID абонента
    customer_id = int()
    
    # Подключение к БД
    try:  
        cur, con = db_connect()
    except Exception as e:
        print(f'Ошибка подключения к БД. {e}')
        return 0
        
    # Получаем ID абонента 
    try:
        customer_data_query = cur.execute(f"SELECT CUSTOMER_ID FROM CUSTOMER WHERE ACCOUNT_NO='{abonent}'")
        query_result = str(customer_data_query.fetchone())
        customer_id = query_result.split('(')
        customer_id = str(customer_id[1]).split(',)')
        customer_id = int(customer_id[0])
    except Exception as e:
        print(f'не удалось получить ID абонента. {e}')
        return 3
        
    # Обновление данных о статусе услуги абонента в таблице CUSTOMER
    try:
        pass
        #customer_data_query = cur.execute(f"UPDATE CUSTOMER SET CUST_STATE=1, ACTIVIZ_DATE='{current_date}' WHERE ACCOUNT_NO='{abonent}'")
        #con.commit()
    except Exception as e:
        print(f'Не удадось выполнить обновление данных о статусе услуги абонента. {e}')
        return 3
        
    # Включение услуги абонента
    try:
        customer_data_query = cur.execute(f"UPDATE SUBSCR_SERV SET STATE_SGN=1, STATE_SRV=13, STATE_DATE='{current_date}' WHERE CUSTOMER_ID={customer_id} AND SERV_ID=1")
        con.commit()
    except Exception as e:
        print(f'Не удалось включить услугу абоненту. {e}')
        return 3
        
    # Внесение записи о включенной услуге в таблицу разовых услуг
    try:
        add_single_service(customer_id)
    except Exception as e:
        print(f'Не удалось добавить запись в таблицу SUBSCR_SERV. {e}')
        return 3

# Добавление разовой услуги
def add_single_service(customer_id:int):
    current_date = str(datetime.today().strftime("%d.%m.%Y"))
    # Соединение с БД
    try:
        cur, con = db_connect()
    except Exception as e:
        print(f'Не удалось подключиться к базе данных. {e}')
        return 4

    #Получаем текущий идентификатор SINGLE_SERVICE_ID
    try:
        select_max = 'SELECT MAX(SINGLE_SERVICE_ID) FROM SINGLE_SERV'
        result_max_query = cur.execute(select_max)
        # Обработка текущего идентификатора
        max_service_id = result_max_query.fetchall()
        max_service_id = str(max_service_id).split('[(')[1]
        max_service_id = int(str(max_service_id).split(',)]')[0])
        # Вычисление идентификатора HISTORY_ID для таблицы SINGLE_SERV
        history_id = max_service_id - 1
        print(f"max:{max_service_id} , history_id: {history_id}")
    except Exception as e:
        print(f'Не удалось получить идентификатор SINGLE_SERVICE_ID. {e}')
        return 4
                   
    # Получение актуального SINGLE_SERV.HISTORY_ID для вставки в SUBSCR_HIST.SUBSCR_SERV_ID
    try:
        query_subscr_serv_id = cur.execute(f'SELECT FIRST 1 HISTORY_ID FROM SINGLE_SERV WHERE CUSTOMER_ID = {customer_id} AND SERVICE_ID = 15 ORDER BY SINGLE_SERVICE_ID ASC')
        subscr_serv_id = query_subscr_serv_id.fetchone()
        subscr_serv_id = str(subscr_serv_id).split('(')[1]
        subscr_serv_id = str(subscr_serv_id).split(',)')[0]
        if subscr_serv_id == None:
            print('Данные не получены')
            subscr_serv_id = history_id - 1
        else:
            pass
    except Exception as e:
        print(f'Не удалось получить HISTORY_ID абонента {customer_id}')
        return 4

    # Добавление записи в таблицу SINGLE_SERV
    try:
        insert_query = cur.execute(f"INSERT INTO SINGLE_SERV(CUSTOMER_ID, SERVICE_ID, HISTORY_ID, NOTICE) \
            VALUES('{customer_id}','13','{history_id}','I')")
        con.commit()
    except Exception as e:
        print(f'Не удалось добавить запись в таблицу разовых услуг(SINGLE_SERV). {e}')
        return 0
        
    # Добавление записи в таблицу историй услуг
    try:
        add_history_service(history_id, customer_id, subscr_serv_id)
    except Exception as e:
        print(f'Не удалось добавить запись в таблицу историй услуг. {e}')
        return e
        
# Добавление истории услуг в таблицу SUBSCR_HIST
def add_history_service(history_id: int ,customer_id:int, subscr_serv_id:int):
    current_date = str(datetime.today().strftime("%d.%m.%Y"))
    # Соединение с БД
    try:
        cur, con = db_connect()
    except Exception as e:
        print(f'Не удалось подключиться к базе данных. {e}')
        return e
        
    try:
        insert_query = cur.execute(f"INSERT INTO SUBSCR_HIST(SUBSCR_HIST_ID, CUSTOMER_ID, SERV_ID, SUBSCR_SERV_ID, DATE_TO, ACT_SERV_ID,\
                                   DISACT_SERV_ID,CLOSED_BY,CLOSED_ON) \
                                   VALUES('{history_id}','{customer_id}','1','{subscr_serv_id}','{current_date}','13','-1','SYSDBA','{current_date}')")
        con.commit()
    except Exception as e:
        print(f'Не удалось добавить запись в таблицу истории услуг(SUBSCR_HIST). {e}')
        return 5

#update_service_customer(91)
#get_customer_data(117)



