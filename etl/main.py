from time import sleep

from etl.etl_pipelines import ETL

if __name__ == '__main__':
    etl = ETL()
    while True:
        etl.run()
        sleep(120)
