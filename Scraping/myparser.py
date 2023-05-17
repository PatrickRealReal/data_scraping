import pandas as pd
import re
import requests

# file_1 = '1-1-2022.xlsx'
# file_2 = '2-1-2022.xlsx'
# file_3 = '3-1-2022.xlsx'

class Solution:

    def __init__(self):
        _url = ''

    def scrapper(self):
        data1 = requests.get('https://www.enecho.meti.go.jp/statistics/electric_power/ep002/xls/2022/1-1-2022.xlsx')
        data2 = requests.get('https://www.enecho.meti.go.jp/statistics/electric_power/ep002/xls/2022/2-1-2022.xlsx')
        data3 = requests.get('')
        print(data1)
        print(data2)


    def fullwidth_to_halfwidth(s):
        FULLWIDTH_TO_HALFWIDTH = str.maketrans('１２３４５６７８９０',
                                               '1234567890')
        return s.translate(FULLWIDTH_TO_HALFWIDTH)


    def gen_basic(month, detCol, xlsxName=''):
        opMonth = pd.to_datetime(month+'.01', format='%Y.%m.%d').date()

        # load file_2 data
        df_2 = pd.read_excel(file_2, sheet_name=month, skiprows=3, header=None)
        df_2 = df_2[:df_2[df_2[0] == '合計'].index.values[0]]
        df_2.fillna(0, inplace=True)

        # load file_1 data
        df_1 = pd.read_excel(file_1, sheet_name=month, skiprows=4, header=None)
        df_1 = df_1[:df_1[df_1[0] == '合　計'].index.values[0]]

        df_1.fillna(0, inplace=True)

        # process file 1 header
        df_1_header = pd.read_excel(file_1, sheet_name=month, header=None, nrows=4)
        str_dt = fullwidth_to_halfwidth(df_1_header.iloc[0, 43])

        m = re.match('(\d+)\D+(\d+)\D+(\d+)\D+', str_dt)
        publishDate_1 = pd.to_datetime(
            '-'.join(list(m.groups())), format='%Y-%m-%d').date()

        # fill horizontally for na
        df_1_header.fillna(method='ffill', axis=1, inplace=True)
        df_1_header.fillna(method='ffill', inplace=True)  # fill vertically for na
        df_1_header = df_1_header[1:]

        # for merged cells
        df_1_header.iloc[1, 30] = df_1_header.iloc[0, 30]
        df_1_header.iloc[1, 31] = df_1_header.iloc[0, 31]

        df_1_header.iloc[1, 44] = df_1_header.iloc[0, 44]
        df_1_header.iloc[1, 45] = df_1_header.iloc[0, 45]

        merged_cols = list(range(8, 30))+list(range(32, 44))

        tech_head = df_1_header.iloc[0, merged_cols] + \
            '_' + df_1_header.iloc[1, merged_cols]
        df_1_header = df_1_header.append(tech_head, ignore_index=True)
        df_1_header.rename(index={2: 4}, inplace=True)
        df_1_header.sort_index(inplace=True)
        df_1_header.fillna(method='ffill', inplace=True)  # fill vertically for na

        lst_dicts = []
        for ridx, row in df_1.iterrows():
            companyName = ''
            # print(row)
            if row[detCol] != '○':
                continue
            companyName = row[0]
            j = 8  # for df_2 col index
            for i in range(8, 48, 2):
                d1 = {}
                d1['OpMonth'] = opMonth
                d1['CompanyNameLocal'] = companyName
                d1['Technology'] = df_1_header.iloc[2, i]
                d1['NumberOfPlant'] = row[i]
                d1['MaxOutputMW'] = row[i+1]
                d1['GenMW'] = df_2.iloc[ridx, j]
                d1['PublishDate'] = publishDate_1
                lst_dicts.append(d1)
                j += 1
            # break

        df_result = pd.DataFrame(lst_dicts)
        if xlsxName:
            df_result.to_excel('output/{}.xlsx'.format(xlsxName), index=False)

        return df_result


    def gen_7(month, xlsxName=''):
        opMonth = pd.to_datetime(month+'.01', format='%Y.%m.%d').date()

        df_3 = pd.read_excel(file_3, sheet_name=month, skiprows=6, header=None)
        cols = list(range(0, 29)) + list(range(37, df_3.shape[0]))
        df_3 = df_3.iloc[cols]
        df_3.reset_index(drop=True, inplace=True)
        df_3 = df_3[:df_3[df_3[0] == '合　計'].index.values[0]]
        df_3.fillna(0, inplace=True)

        df_3_header = pd.read_excel(file_3, sheet_name=month, header=None, nrows=6)
        df_3_header.fillna(method='ffill', inplace=True)  # fill vertically for na

        str_dt = fullwidth_to_halfwidth(df_3_header.iloc[0, 16])

        m = re.match('(\d+)\D+(\d+)\D+(\d+)\D+', str_dt)
        publishDate_3 = pd.to_datetime(
            '-'.join(list(m.groups())), format='%Y-%m-%d').date()

        lst_dicts = []
        for ridx, row in df_3.iterrows():
            companyName = ''
            # print(row)
            companyName = row[0]

            str_bits = ''
            for i in range(1, 8):
                if row[i] == '○':
                    str_bits += '1'
                else:
                    str_bits += '0'

            for i in range(8, 20):
                d1 = {}
                d1['OpMonth'] = opMonth
                d1['CompanyName'] = companyName
                d1['CompanyType'] = str_bits
                d1['DemandType'] = df_3_header.iloc[5, i]
                d1['DemandMW'] = row[i]
                d1['PublishDate'] = publishDate_3
                lst_dicts.append(d1)
        df_result = pd.DataFrame(lst_dicts)

        if xlsxName:
            df_result.to_excel('output/{}.xlsx'.format(xlsxName), index=False)
        return df_result

sol = Solution()
sol.scrapper()