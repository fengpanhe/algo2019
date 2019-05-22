import csv
import re
import os
from data_load import AdStatic, AdOp
import pandas as pd
import time


class TrainData(object):
    def __init__(self):
        self.log_file = '../data/totalExposureLog.out'
        self.csv_file = '../data/train_data.csv'
        self.headers = [
            'EcpmNum', 'AdId', 'CreateTime', 'AdSize', 'IndustryId',
            'CommodityType', 'CommodityId', 'AccountId', 'UpTimeStage',
            'TargetPeople', 'price'
        ]
        if not os.path.exists(self.csv_file):
            self.to_csv()

    def to_csv(self):
        ad_count = self.ad_count()
        print('ad count:', len(ad_count))
        ad_s = AdStatic()
        ad_o = AdOp()
        csv_f = open(self.csv_file, 'w')
        csv_f.write(','.join(self.headers))
        csv_f.write('\n')
        effective_count = 0
        for key, value in ad_count.items():
            ads = ad_s.get(value['ad_id'])
            if ads is None:
                continue
            week = value['week']
            ad_request_time = value['ad_request_time']
            price = value['price']
            target_people = '0'
            uptime_stage = '281474976710655'
            for row in ad_o.get(int(value['ad_id'])).itertuples():
                UpdateField = getattr(row, "UpdateField")
                row_value = getattr(row, "value")
                if getattr(row, "Date") < int(ad_request_time):
                    if UpdateField == 1:
                        print(1)
                    elif UpdateField == 2:
                        price = int(row_value)
                    elif UpdateField == 3:
                        target_people = row_value
                    elif UpdateField == 4:
                        ts = row_value.split(' ')
                        index = (week + 1) % len(ts)
                        uptime_stage = ts[index]
            ss = ''
            ss += str(value['count']) + ','
            ss += value['ad_id'] + ','
            ss += str(ads['Date']) + ','
            ss += str(ads['AdSize']) + ','
            ss += str(ads['IndustryId']) + ','
            ss += str(ads['CommodityType']) + ','
            ss += str(ads['CommodityId']) + ','
            ss += str(ads['AccountId']) + ','
            ss += uptime_stage + ','
            ss += target_people + ','
            ss += str(price)
            csv_f.write(ss)
            csv_f.write('\n')
            effective_count += value['count']
        csv_f.close()
        print('effective_count:', effective_count)

    def ad_count(self):
        ad_count = {}
        source_f = open(self.log_file, 'r')
        for line in source_f.readlines():
            l = re.split(r'[\t\n]', line)
            while '' in l:
                l.remove('')
            if len(l) < 10 or int(l[1]) == 0:
                continue
            ad_request_id, ad_request_time, ad_local_id, user_id, ad_id, \
                ad_size, ad_price, pctr, quality_ecpm, totalEcpm = l
            ad_request_time = int(ad_request_time)
            ymd = time.strftime("%Y%m%d", time.localtime(ad_request_time))
            week = time.strftime("%w", time.localtime(ad_request_time))
            key = ad_id + ymd
            if key in ad_count:
                ad_count[key]['count'] += 1
            else:
                ad_count[key] = {
                    'ad_id': ad_id,
                    'count': 1,
                    'week': int(week),
                    'ad_request_time': ad_request_time,
                    'price': ad_price
                }
        source_f.close()
        return ad_count


if __name__ == '__main__':
    train_dataset = TrainData()
    train_dataset.to_csv()
