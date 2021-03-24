import psycopg2
import pandas as pd
import csv
import logging
import time



def add_year(filename,year):
    df = pd.read_csv(filename, sep=';', header=0, encoding='cp1252')
    df["year"] = year
    df.to_csv(filename, index=False, encoding='cp1252', sep=';')



def copy_data(db_cursor):
    db_cursor.execute("""SELECT COUNT(*) FROM zno""")
    if list(db_cursor)==[(0,)]:
        log.info("Copying data from the first file")
        with open('datka2019.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            db_cursor.copy_from(file=f, table='zno', sep=';', null='')

        log.info("Copying data from the second file")
        with open('datka2020.csv', 'r') as f1:
            reader = csv.reader(f1)
            next(reader)
            db_cursor.copy_from(file=f1, table='zno', sep=';', null='')




def max_ball(db_cursor):
    log.info("Getting max from math_100 column")
    ball = "SELECT max(math_100), year FROM zno WHERE math_test_stat = 'Зараховано' GROUP BY year"
    with open('math_ball_100.csv', 'w') as f2:
        db_cursor.copy_expert(f"COPY ({ball}) TO STDOUT WITH CSV HEADER", f2)
        log.info("Result copied to a math_ball_100 csv file")

log = logging.getLogger(__name__)
logging.basicConfig(filename="db_log.txt",
                    level=logging.INFO,
                    format="%(asctime)s *** %(message)s")
log.info("Start of program")

def main():
    conn = psycopg2.connect(
        host='192.168.0.105',
        database='postgres',
        user='postgres',
        password='postgres'

    )
    db_cursor = conn.cursor()
    db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS zno (
            out_id VARCHAR PRIMARY KEY,
            birth VARCHAR,
            sex VARCHAR,
            region VARCHAR,
            area VARCHAR,
            tername VARCHAR,
            reg_type VARCHAR,
            ter_type VARCHAR,
            class_profile VARCHAR,
            class_lang VARCHAR,
            EOName VARCHAR,
            EOType VARCHAR,
            EOReg VARCHAR,
            EOArea VARCHAR,
            EOTer VARCHAR,
            EOParent VARCHAR,
            ukr_test VARCHAR,
            ukr_test_stat VARCHAR,
            ukr_100 FLOAT,
            ukr_12 FLOAT,
            ukr_ball FLOAT,
            ukr_adapt FLOAT,
            ukrPTName VARCHAR,
            ukrPTReg VARCHAR,
            ukrPTArea VARCHAR,
            ukrPTTer VARCHAR,
            hist_test VARCHAR,
            hist_lang VARCHAR,
            hist_test_stat VARCHAR,
            hist_100 FLOAT,
            hist_12 FLOAT,
            hist_ball FLOAT,
            histPTName VARCHAR,
            histPTReg VARCHAR,
            histPTArea VARCHAR,
            histPTTer VARCHAR,
            math_test VARCHAR,
            math_lang VARCHAR,
            math_test_stat VARCHAR,
            math_100 FLOAT,
            math_12 FLOAT,
            math_ball FLOAT,
            mathPTName VARCHAR,
            mathPTReg VARCHAR,
            mathPTArea VARCHAR,
            mathPTTer VARCHAR,
            phys_test VARCHAR,
            phys_lang VARCHAR,
            phys_test_stat VARCHAR,
            phys_100 FLOAT,
            phys_12 FLOAT,
            phys_ball FLOAT,
            physPTName VARCHAR,
            physPTReg VARCHAR,
            physPTArea VARCHAR,
            physPTTer VARCHAR,
            chem_test VARCHAR,
            chem_lang VARCHAR,
            chem_test_stat VARCHAR,
            chem_100 FLOAT,
            chem_12 FLOAT,
            chem_ball FLOAT,
            chemPTName VARCHAR,
            chemPTReg VARCHAR,
            chemPTArea VARCHAR,
            chemPTTer VARCHAR,
            bio_test VARCHAR,
            bio_lang VARCHAR,
            bio_test_stat VARCHAR,
            bio_100 FLOAT,
            bio_12 FLOAT,
            bio_ball FLOAT,
            bioPTName VARCHAR,
            bioPTReg VARCHAR,
            bioPTArea VARCHAR,
            bioPTTer VARCHAR,
            geo_test VARCHAR,
            geo_lang VARCHAR,
            geo_test_stat VARCHAR,
            geo_100 FLOAT,
            geo_12 FLOAT,
            geo_ball FLOAT,
            geoPTName VARCHAR,
            geoPTReg VARCHAR,
            geoPTArea VARCHAR,
            geoPTTer VARCHAR,
            eng_test VARCHAR,
            eng_test_stat VARCHAR,
            eng_100 FLOAT,
            eng_12 FLOAT,
            eng_dpa VARCHAR,
            eng_ball FLOAT,
            engPTName VARCHAR,
            engPTReg VARCHAR,
            engPTArea VARCHAR,
            engPTTer VARCHAR,
            fra_test VARCHAR,
            fra_test_stat VARCHAR,
            fra_100 FLOAT,
            fra_12 FLOAT,
            fra_dpa VARCHAR,
            fra_ball FLOAT,
            fraPTName VARCHAR,
            fraPTReg VARCHAR,
            fraPTArea VARCHAR,
            fraPTTer VARCHAR,
            deu_test VARCHAR,
            deu_test_stat VARCHAR,
            deu_100 FLOAT,
            deu_12 FLOAT,
            deu_dpa VARCHAR,
            deu_ball FLOAT,
            deuPTName VARCHAR,
            deuPTReg VARCHAR,
            deuPTArea VARCHAR,
            deuPTTer VARCHAR,
            spa_test VARCHAR,
            spa_test_stat VARCHAR,
            spa_100 FLOAT,
            spa_12 FLOAT,
            spa_dpa VARCHAR,
            spa_ball FLOAT,
            spaPTName VARCHAR,
            spaPTReg VARCHAR,
            spaPTArea VARCHAR,
            spaPTTer VARCHAR,
            year VARCHAR)
             """)

    add_year('datka2019.csv', 2019)
    add_year('datka2020.csv', 2020)


    copy_data(db_cursor)


    conn.commit()

    max_ball(db_cursor)
    db_cursor.close()
    conn.close()
    log.info("Finished")


if __name__=="__main__":
    try:
        main()
    except TypeError:
        pass
