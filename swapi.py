import requests
import pandas as pd
import mysql.connector as msql
from mysql.connector import Error
import argparse
import getpass
import logging


def add_data(data, resident, planet_name, starship_name):
    data.append({'name': resident['name'],
                 'homeworld_name': planet_name,
                 'height': resident['height'],
                 'gender': resident['gender'] if resident['gender'] != 'n/a' else 'None',
                 'starships_name': starship_name})
    return 0


def read_data(planet_name):
    session = requests.Session()
    adapter = requests.sessions.HTTPAdapter(
        pool_connections=100,
        pool_maxsize=100)
    session.mount('http://', adapter)
    res = session.get('https://swapi.dev/api/planets/?search=' + planet_name)

    if res.json()['count'] != 1:
        raise Exception('There is no such a planet! Try again.')

    residents = res.json()['results'][0]['residents']

    if len(residents) == 0:
        raise Exception('There is no residents on this planet! Try again.')

    data = []
    for i in range(len(residents)):
        res = session.get(residents[i])
        resident = res.json()
        for j in range(len(resident['starships'])):
            res = session.get(resident['starships'][j])
            starship = res.json()['name']
            add_data(data, resident, planet_name, starship)

        if len(resident['starships']) == 0:
            add_data(data, resident, planet_name, 'None')

    tab = pd.DataFrame(data=data)
    return tab


def from_csv_to_mysql(path_to_csv, host, user, pwd, database, table):
    tab = pd.read_csv(path_to_csv, index_col=False)
    try:
        mydb = msql.connect(host=host, user=user, password=pwd)
        if not mydb.is_connected():
            raise Exception('The connection to MySQL Server is not available!')

        logging.basicConfig(filename='swapi.log', format='%(asctime)s - %(message)s', level=logging.INFO)
        cursor = mydb.cursor()
        cursor.execute('DROP DATABASE IF EXISTS ' + database)
        cursor.execute('CREATE DATABASE ' + database)
        logging.info("Database is created.")
        cursor.execute('USE ' + database)
        cursor.execute('DROP TABLE IF EXISTS ' + table)
        cursor.execute('CREATE TABLE ' + table + '(name varchar(100), homeworld_name varchar(100),'
                                                 'height int, gender varchar(10), starships_name varchar(100))')
        logging.info("Table is created.")

        for i, row in tab.iterrows():
            sql = 'INSERT INTO ' + database + '.' + table + ' VALUES (%s,%s,%s,%s,%s)'
            cursor.execute(sql, tuple(row))
            mydb.commit()
        logging.info("Records are inserted.")

        print('Data from csv-file was added in MySQL.')
    except Error as e:
        print("Error while connecting to MySQL:", e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--planet', type=str, default='Tatooine',
                        help='The name of the planet - the homeworld for the characters to be selected.'
                             'Default: Tatooine.')
    parser.add_argument('--csvname', type=str, default='swapi.csv',
                        help='Name for csv-file or file path. Default: swapi.csv.')
    parser.add_argument('--host', required=True, type=str, help='MySQL host.')
    parser.add_argument('--user', required=True, type=str, help='MySQL username.')
    parser.add_argument('--db', type=str, default='swapi',
                        help='MySQL database name for creation. Default: swapi.')
    parser.add_argument('--tab', type=str, default='swapi_data',
                        help='MySQL table name for creation. Default: swapi_data.')
    args = parser.parse_args()

    password = getpass.getpass('MySQL user password: ')

    tab = read_data(args.planet)
    tab.to_csv(path_or_buf=args.csvname, index=False)
    from_csv_to_mysql(args.csvname, args.host, args.user, password, args.db, args.tab)
