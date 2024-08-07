# Мои проекты: 

## 1. [Анализ рекламных кампаний игры](https://github.com/maranafaamen/portfolio/tree/main/gamedev_marketing_campaigns)

![cover](/gamedev_marketing_campaigns/assets/plot1.png)

Цель анализа - определить оптимальный срок окупаемости, а также слабые места рекламных кампаний игры.
 
### Задачи
1. Определить оптимальный срок оценки окупаемости рекламных кампаний;
2. Оценить успешность каждой кампании;
3. Рассчитать накопительный APRU для 7, 14 и 30 дня жизни пользователей;
4. Рассчитать Cost Per Install для кампаний.

### Инструменты

- **Pandas** - Создание датафреймов и расчет метрик;

- **Matplotlib / Seaborn** - Построение графиков и диаграмм.

## 2. [RFM-анализ клиентской базы аптечной сети](https://github.com/maranafaamen/portfolio/tree/main/rfm_pharmacy)

![cover](/rfm_pharmacy/assets/monetary_dist.png)

Цель анализа - дать рекомендации по повышению эффективности SMS-рассылки.
 
### Задачи
1. Провести RFM-сегментацию клиентов.
2. Проанализировать сформированные RFM-группы и дать рекомендации для каждой из них.
3. Сделать выводы об успешности программы лояльности.

### Инструменты

- **PostgreSQL** - формирование SQL-запросов.

- **Metabase** - визуализация результатов SQL-запросов.

## 3. [ETL-скрипт для загрузки данных для онлайн-школы](https://github.com/maranafaamen/portfolio/tree/main/edtech_etl)

![cover](/edtech_etl/assets/cover.png)

### Задачи
1. Написать ETL-скрипт для получения данных по API онлайн-школы, их обработки и загрузки в БД на PostgreSQL.
2. Реализовать логирование.
3. Добавить дополнительные фичи - агрегацию данных за день в Google Sheets и отправку уведомлений на email.

### Инструменты

- **PostgreSQL** - создание БД и таблиц в ней.
- **Python** - реализация пайплайна.
- requests - для работы с веб-сервисами.
- gspread - для работы с Google Sheets.
- paramiko, sshtunnel - для подключения к серверу по SSH.
- psycopg2 - для работы с PostgreSQL.
