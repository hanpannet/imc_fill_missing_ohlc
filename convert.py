import datetime
import unittest
from pprint import pprint

import click
import numpy as np
import pandas as pd

time_format = '%Y-%m-%d %H:%M'
row_format = '{},{:.5f},{:.5f},{:.5f},{:.5f}\n'
COLUM_NAMES = ['time', 'start', 'high', 'low', 'close']


def ite_time(start):
    while True:
        start += datetime.timedelta(minutes=1)
        yield start


class Processor:
    def __init__(self, n, filename='./output/m{}.csv'):
        self.n = n
        self.filename = filename.format(n)

        with open(self.filename, 'w') as f:
            f.write('')

    def proceed_minute(self, row_time, row):
        # print('proceed minute', row_time, row.time)
        if row_time.minute % self.n == 0:
            str_time = datetime.datetime.strftime(row_time, time_format)
            with open(self.filename, 'a') as f:
                f.write(row_format.format(
                    str_time, row.start, row.high, row.low, row.close
                ))


def main(minutes, test=False):
    ite = ite_time(datetime.datetime(2000, 5, 30, 17, 57))
    df = pd.read_csv(
        './data/usdjpy_1min.csv' if not test else './test_data/usdjpy_1min.csv',
        header=None, encoding='utf-8',
        names=COLUM_NAMES,
        dtype={0: str, 1: np.float64, 2: np.float64,
               3: np.float64, 4: np.float64}
    )
    p = Processor(
        minutes,
        filename='./output/m{}.csv' if not test else './test_output/m{}.csv'
    )
    now = next(ite)
    prev_row = df.loc[0]
    print('start:', now)
    for i, row in df.iterrows():
        row_time = datetime.datetime.strptime(row.time, time_format)
        # print(now, row_time)
        while now < row_time:
            p.proceed_minute(now, prev_row)
            now = next(ite)
        # # Suppose the missing rows have prev row's close value for all fields.
        # prev_row = pd.Series(
        #     [row.time, row.close, row.close, row.close, row.close],
        #     index=COLUM_NAMES
        # )
        prev_row = row
        p.proceed_minute(row_time, row)
        now = next(ite)


@click.command()
@click.option('-m', '--minutes', type=int)
def cli(minutes):
    print(minutes, 'minutes.')
    main(minutes)


if __name__ == '__main__':
    cli()
