import csv
import re
import os
import pandas as pd


class AdStatic(object):
    def __init__(self):
        self.source_data_file = '../../data/ad_static_feature.out'
        self.csv_data_file = '../../data/ad_static_feature.csv'
        self.headers = ['AdId', 'Date', 'AccountId', 'CommodityId', 'CommodityType', 'IndustryId', 'AdSize']
        self.ids = []
        if not os.path.exists(self.csv_data_file):
            self.to_csv()
        self.df = pd.read_csv(self.csv_data_file, header=0, index_col=0)
        # self.df.set_index('AdId')
        print('ad static size:', len(self.df))

    def to_csv(self):
        with open(self.csv_data_file, 'w') as csv_f:
            source_f = open(self.source_data_file, 'r')
            csv_f.write(','.join(self.headers))
            csv_f.write('\n')
            # count1 = 0
            # count2 = 0
            for line in source_f.readlines():
                l = re.split(r'[\t\n]', line)
                while '' in l:
                    l.remove('')
                if len(l) < len(self.headers) or int(l[1]) == 0:
                    continue
                # if ',' in l[5]:
                #     count1 += 1
                # if ',' in l[6]:
                #     count2 += 1
                # l[5] = l[5].replace(',', '|')
                
                ll = ','.join(l)
                if ll.count(',') != (len(self.headers) - 1):
                    continue
                # print(int(l[5]))
                csv_f.write(ll)
                csv_f.write('\n')
            # print('count1:' , count1)
            # print('count2:' , count2)
            source_f.close()

    def get(self, ad_id):
        # print(self.df)
        # return self.df[self.df['AdId'].isin([ad_id])].to_json()
        if int(ad_id) in self.df.index:
            return self.df.loc[int(ad_id)]
        else:
            return None

    def get_df(self):
        return self.df


class AdOp(object):
    def __init__(self):
        self.source_data_file = '../../data/ad_operation.dat'
        self.csv_data_file = '../../data/ad_operation.csv'
        self.headers = ['AdId', 'Date', 'OpType', 'UpdateField', 'value']
        self.ids = []
        if not os.path.exists(self.csv_data_file):
            self.to_csv()
        self.df = pd.read_csv(self.csv_data_file, header=0)
        print('ad op size:', len(self.df))

    def to_csv(self):
        with open(self.csv_data_file, 'w') as csv_f:
            source_f = open(self.source_data_file, 'r')
            csv_f.write(','.join(self.headers))
            csv_f.write('\n')
            for line in source_f.readlines():
                l = re.split(r'[\t\n]', line)
                while '' in l:
                    l.remove('')
                if len(l) < len(self.headers):
                    continue
                l[4] = l[4].replace(',', ' ')
                ll = ','.join(l)
                if ll.count(',') != (len(self.headers) - 1):
                    continue
                csv_f.write(ll)
                csv_f.write('\n')
            source_f.close()

    def get(self, ad_id):
        return self.df[self.df['AdId'].isin([ad_id])]


class UserData(object):
    def __init__(self):
        self.source_data_file = '../../data/user_data'
        self.csv_data_file = '../../data/user_data.csv'
        self.headers = ['UserId', 'Age', 'Gender', 'Area', 'MarriagesStatus', 'Education', 'ConsumptionAbility', 'Device', 'Work', 'ConnectionType', 'Behavior']
        self.ids = []
        if not os.path.exists(self.csv_data_file):
            self.to_csv()
        self.df = pd.read_csv(self.csv_data_file, header=0, index_col=0)

    def to_csv(self):
        with open(self.csv_data_file, 'w') as csv_f:
            source_f = open(self.source_data_file, 'r')
            csv_f.write(','.join(self.headers))
            csv_f.write('\n')
            for line in source_f.readlines():
                l = re.split(r'[\t\n]', line)
                while '' in l:
                    l.remove('')
                if len(l) < len(self.headers):
                    continue
                l[3] = l[3].replace(',', '|')
                l[4] = l[4].replace(',', '|')
                l[8] = l[8].replace(',', '|')
                l[10] = l[10].replace(',', '|')
                ll = ','.join(l)
                if ll.count(',') != (len(self.headers) - 1):
                    continue
                csv_f.write(ll)
                csv_f.write('\n')
            source_f.close()

    def get(self, user_id):
        # return self.df[self.df['UserId'].isin([user_id])]
        return self.df.loc[int(user_id)].to_json()


class LogData(object):
    def __init__(self):
        self.source_data_file = '../../data/totalExposureLog.out'
        self.csv_data_file = '../../data/totalExposureLog.csv'
        self.headers = ['ARID', 'ARTime', 'ALId', 'UserId', 'AdId', 'size', 'bid', 'pctr', 'quality_ecpm', 'totalEcpm']
        if not os.path.exists(self.csv_data_file):
            self.to_csv()
        self.df = pd.read_csv(self.csv_data_file, header=0, index_col=0)

    def to_csv(self):
        with open(self.csv_data_file, 'w') as csv_f:
            source_f = open(self.source_data_file, 'r')
            csv_f.write(','.join(self.headers))
            csv_f.write('\n')
            for line in source_f.readlines():
                l = re.split(r'[\t\n]', line)
                while '' in l:
                    l.remove('')
                if len(l) < len(self.headers):
                    continue
                l[3] = l[3].replace(',', '|')
                l[4] = l[4].replace(',', '|')
                l[8] = l[8].replace(',', '|')
                l[10] = l[10].replace(',', '|')
                ll = ','.join(l)
                if ll.count(',') != (len(self.headers) - 1):
                    continue
                csv_f.write(ll)
                csv_f.write('\n')
            source_f.close()

    def get(self, ad_id):
        # return self.df[self.df['UserId'].isin([user_id])]
        return self.df[self.df['AdId'].isin([ad_id])]


if __name__ == '__main__':
    ad_s = AdStatic()
    ad_s.to_csv()
    # res = ad_s.get('77500')
    # print(res)
    # print(type(res))

    # ad_o = AdOp()
    # ad_o.to_csv()
    # print(ad_o.get(593323).loc[4])
    # print(ad_o.get(593323))
    # for row in ad_o.get(593323).iterrows():
    #     print(item)
    # user_data = UserData()
    # print(user_data.get('624218'))
