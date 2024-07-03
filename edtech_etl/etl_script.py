import requests
import json
import psycopg2
import sshtunnel
import logging
import os
import re
import gspread
import smtplib
import ssl
from email.message import EmailMessage
from datetime import datetime, date, timedelta

# Добавляем необходимые параметры подключений из файлов

with open("itresume_api.json", "r") as api_file, \
        open("ssh.json", "r") as ssh_file, \
        open("db.json", "r") as db_file, \
        open("email_params.json", "r") as email_file:

    api_params = json.loads(api_file.read())
    ssh_params = {
        key: tuple(value) if isinstance(value, list) else value
        for key, value in json.loads(ssh_file.read()).items()
    }
    db_params = json.loads(db_file.read())
    email_params = json.loads(email_file.read())

gspread_file = 'service_account.json'

# Описание классов

# Основной класс менеджера


class ETLManager:
    def __init__(self, api_params, ssh_params, db_params, gspread_file):
        self.api_params = api_params
        self.ssh_params = ssh_params
        self.db_params = db_params
        self.gspread_file = gspread_file
        self.log_dir = 'logs'
        self.setup_logging()

    def setup_logging(self):
        log_filename = os.path.join(
            self.log_dir,
            f'etl_{datetime.now().strftime("%Y-%m-%d")}.log'
        )
        logging.basicConfig(
            format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO,
            filename=log_filename
        )

    def delete_old_logs(self):
        for file in os.listdir(self.log_dir):
            log_date = date.fromisoformat(
                re.search(r'\d{4}-\d{2}-\d{2}', file).group()
            )
            if log_date - date.today() < -timedelta(2):
                os.remove(os.path.join(self.log_dir, file))
    
    def send_email(self, email_params, recipient_email):
        
        smtp_server = email_params['smtp_server']
        port = email_params['port']
        sender_email = email_params['sender_email']
        password = email_params['password']
        
        try:
            
            logging.info(f'Отправка сообщения на почту {recipient_email} ... ')
    
            context=ssl.create_default_context()
    
            message = EmailMessage()
    
            body = f"""Автоматическое сообщение от ETL-менеджера:
                
{self.result}

Подробную информацию можно получить в лог-файле.

Это информационное сообщение, не отвечайте на него"""

            message.set_content(body)
            message['Subject'] = 'Отчет о загрузке файлов'
            message['From'] = sender_email
            message['To'] = recipient_email
    
            with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    
                server.login(sender_email, password)
                
                server.send_message(msg=message)
                
            logging.info('Сообщение доставлено!')
                
        except Exception as err:
            
            logging.error(f'Сообщение не доставлено: {err}')

   # Основной скрипт

    def run_etl(self, start, end):
        
        self.result = '*Обработка данных IT Resume завершена с ошибкой*'
            
        # Удаляем старые логи
    
        self.delete_old_logs()
    
        # Подключаемся к API и получаем данные
    
        api_client = APIClient(api_params)
    
        if api_client.check_connection():
    
            attempts = api_client.get_data(start, end)
    
        # Обрабатываем данные
    
        handler = DataHandler()
    
        data_to_upload = handler.transform_data(attempts)
    
        # Создаем подключение к БД и загружаем обработанные данные
    
        connector = DatabaseConnector(ssh_params, db_params)
    
        if connector.check_connection():
    
            with connector:
    
                handler.load_data(data_to_upload, connector.connection)
                    
                # Агрегируем данные за день
            
                handler.calculate_daily_metrics(
                    connector.connection, 
                    start, 
                    end, 
                    self.gspread_file)
            
                self.result = '*Данные от IT Resume успешно обработаны и загружены*'

# Класс для работы с API


class APIClient:
    def __init__(self, api_params):
        self.api_params = api_params

    def get_data(self, start, end):

        try:

            logging.info('Загрузка данных ... ')

            api_params['params']['start'] = start
            api_params['params']['end'] = end

            response = requests.get(**api_params)

            response.raise_for_status()

            logging.info('Загрузка успешно завершена!')

            return response.json()

        except requests.exceptions.RequestException as err:

            logging.error(f'Ошибка при загрузке данных: {err}')

    def check_connection(self):
        try:

            logging.info('Проверка подключения к API ... ')

            response = requests.head(**api_params)
            response.raise_for_status()

            logging.info('Подключение к API прошло успешно!')

            return True

        except requests.exceptions.RequestException as err:

            logging.error(f'Ошибка при подключении к API: {err}')

            return False

# Класс для обработки и загрузки данных в БД


class DataHandler:

    def transform_data(self, data):

        try:

            logging.info('Обработка файлов по шаблону ...')

            upload_lst = []

            for attempt in data:
                passback_params = json.loads(
                    attempt['passback_params'].replace("'", '"'))
                attempt_data = {
                    'user_id': attempt['lti_user_id'],
                    'oauth_consumer_key':
                    passback_params['oauth_consumer_key'],
                    'lis_result_sourcedid':
                    passback_params['lis_result_sourcedid'],
                    'lis_outcome_service_url':
                    passback_params.get('lis_outcome_service_url'),
                    'is_correct': bool(attempt['is_correct']),
                    'attempt_type': attempt['attempt_type'],
                    'created_at': attempt['created_at'],
                }
                attempt_cleaned_data = {
                    k: v if v else None for k, v in attempt_data.items()}
                upload_lst.append(attempt_cleaned_data)

        except Exception as err:

            logging.error(f'Ошибка при обработке файлов: {err}')

        else:

            logging.info('Файлы обработаны успешно!')

            return upload_lst
    
    def calculate_daily_metrics(self, connection, start, end, gspread_file):
        
        start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        end = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        delta = end - start

        dates  = []
        for i in range(delta.days + 1):
            day = start + timedelta(days=i)
            dates.append(day.strftime('%Y-%m-%d'))
        
        try:
            
            logging.info('Обработка файлов за день ...')

            with connection:

                with connection.cursor() as cur:
                    
                    for date in dates:
                    
                        cur.execute("""
                                    select
                                    	to_char(created_at, 'YYYY-MM-DD') as date
        	                            , count(*) as total_attempts
    	                                , count (case
    		                                when attempt_type = 'submit' then 1 
                                            end) as submits
    	                                , count(is_correct) as correct_submits
    	                                , count(distinct user_id) as unique_users
                                    from 
    	                                students_attempts
                                    group by to_char(created_at, 'YYYY-MM-DD')
                                    having to_char(created_at, 'YYYY-MM-DD') = %s
                            """, (date, ))
                        
                    res = cur.fetchall()
            
            try:
                
                logging.info('Загрузка данных в Google Sheets ... ')
                
                client = gspread.service_account(gspread_file)
            
                sh = (client
                     .open('Статистика по студентам SkillFactory')
                     .worksheet('Статистика по IT Resume'))

                cell_list = sh.col_values(1)
                    
                for row in res:
                    
                    try:
                        row_index = cell_list.index(row[0]) + 1
                            
                    except ValueError:
                        row_index = None
        
                    if row_index:
                        sh.update_cell(row_index, 2, row[1])
                        sh.update_cell(row_index, 3, row[2])
                        sh.update_cell(row_index, 4, row[3])
                        sh.update_cell(row_index, 5, row[4])
                    else:
                        sh.append_row(row)
                
            except Exception as err:

                logging.error(f'Ошибка при обработке файлов за день: {err}')
            
        except Exception as err:

            logging.error(f'Ошибка при обработке файлов за день: {err}')
        
        else:
            
            logging.info('Загрузка данных успешно завершена!')
                    

    def create_table(self, connection):

        logging.info(
            'Проверяем существование таблицы students_attempts ...')

        # Проверяем, существует ли таблица students_attempts

        with connection:

            with connection.cursor() as cur:

                cur.execute("""
                            select exists (
                                select from information_schema.tables
                                where table_name = 'students_attempts'
                                );
                            """)
                table_exists = cur.fetchone()[0]

                # Создаем таблицу, если она не существует

                if not table_exists:

                    logging.info('Создаем таблицу students_attempts')

                    cur.execute("""
                                create table students_attempts (
                                    id serial primary key,
                                    user_id text,
                                    oauth_consumer_key text,
                                    lis_result_sourcedid text,
                                    lis_outcome_service_url text,
                                    is_correct boolean,
                                    attempt_type text,
                                    created_at timestamp
                                    );
                                """)

    def load_data(self, data, connection):

        self.create_table(connection)

        # Записываем данные в таблицу

        logging.info('Загружаем данные в БД ... ')

        try:

            with connection:

                with connection.cursor() as cur:

                    for attempt in data:
                        cur.execute("""
                                    insert into students_attempts (
                                        user_id, 
                                        oauth_consumer_key,
                                        lis_result_sourcedid,
                                        lis_outcome_service_url,
                                        is_correct,
                                        attempt_type,
                                        created_at
                                        )
                                    values (
                                        %(user_id)s,
                                        %(oauth_consumer_key)s,
                                        %(lis_result_sourcedid)s,
                                        %(lis_outcome_service_url)s,
                                        %(is_correct)s,
                                        %(attempt_type)s,
                                        %(created_at)s
                                        );
                                    """, attempt)
        except Exception as err:

            logging.error(f'Ошибка при загрузке в БД: {err}')

        else:

            logging.info('Загрузка данных успешно завершена!')

# Класс для подключения к БД


class DatabaseConnector:
    def __init__(self, ssh_params, db_params):
        self.tunnel = sshtunnel.SSHTunnelForwarder(**ssh_params)
        self.database_params = db_params
        self.connection = None

    def __enter__(self):
        self.tunnel.start()
        self.database_params['port'] = self.tunnel.local_bind_port
        self.connection = psycopg2.connect(**self.database_params)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
        self.tunnel.stop()

    def check_connection(self):
        try:

            logging.info('Проверка подключения к БД...')

            # Проверка SSH-подключения

            if not self.tunnel.is_active:
                self.tunnel.start()

            # Проверка подключения к базе данных

            self.database_params['port'] = self.tunnel.local_bind_port
            self.connection = psycopg2.connect(**self.database_params)
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")

            logging.info('Подключение к БД прошло успешно!')

            return True

        except (
                sshtunnel.BaseSSHTunnelForwarderError,
                psycopg2.OperationalError
        ) as err:

            logging.error(f'Ошибка подключения: {err}')

            return False

# Создаем менеджер и выполняем скрипт


etl_manager = ETLManager(api_params, ssh_params, db_params, gspread_file)

etl_manager.run_etl('2023-04-02 9:00:00', '2023-04-02 9:05:00')

etl_manager.send_email(email_params, '8812103@gmail.com')
