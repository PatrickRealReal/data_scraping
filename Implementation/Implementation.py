import datetime
import io

import re
import ssl
import pandas as pd
import psycopg2
import requests

# C: pandas melt func;



class Solution:
    def __init__(self):
        # Transfer to make sure that it could be translated as number
        self._FULLWIDTH_TO_HALFWIDTH = str.maketrans('１２３４５６７８９０',
                                                     '1234567890')
        ssl._create_default_https_context = ssl._create_unverified_context
        self.host = "127.0.0.1"
        self.database = "admin"
        self.user = "postgres"
        self.port = '5432'
        self.password = "Iamxze00"

    def fullwidth_to_halfwidth(self, s):
        return s.translate(self._FULLWIDTH_TO_HALFWIDTH)

    def gen_1_2(self, file_1, file_2, month, detCol, xlsxName=''):
        opMonth = pd.to_datetime(month + '.01', format='%Y.%m.%d').date()

        # load file_2 data
        df_2 = pd.read_excel(file_2, sheet_name=month, skiprows=5, header=None)
        df_2 = df_2[:df_2[df_2[2] == '合計'].index.values[0]]
        df_2.fillna(0, inplace=True)

        # load file_1 data
        df_1 = pd.read_excel(file_1, sheet_name=month, skiprows=5, header=None)
        df_1 = df_1[:df_1[df_1[2] == '合計'].index.values[0]]
        df_1 = df_1.replace(r'^\s*$', 0, regex=True)
        df_1.fillna(0, inplace=True)

        # process file 1 header
        df_1_header = pd.read_excel(file_1, sheet_name=month, header=None, nrows=4)
        str_dt = self.fullwidth_to_halfwidth(df_1_header.iloc[0, 49])

        m = re.match('(\d+)\D+(\d+)\D+(\d+)\D+', str_dt)
        publishDate_1 = pd.to_datetime(
            '-'.join(list(m.groups())), format='%Y-%m-%d').date()

        df_1_header.fillna(method='ffill', inplace=True)  # fill vertically for na
        df_1_header = df_1_header[1:]
        df_1_header.reset_index(drop=True, inplace=True)

        lst_dicts = []
        for ridx, row in df_1.iterrows():
            companyName = ''
            if row[detCol] != '○':
                continue
            companyName = row[2]
            j = 10  # for df_2 col index
            for i in range(10, 50, 2):
                d1 = {'OpMonth': opMonth, 'CompanyNameLocal': companyName, 'FuelType': df_1_header.iloc[0, i],
                      'GenerationType': df_1_header.iloc[1, i], 'NumberOfPlant': row[i],
                      'MaxOutputMW': row[i + 1] / 1000, 'GenMW': df_2.iloc[ridx, j], 'PublishDate': publishDate_1}
                lst_dicts.append(d1)
                j += 1
            # break

        df_result = pd.DataFrame(lst_dicts)
        if df_result.shape[0] > 0:
            df_result = df_result[df_result['FuelType'] != '計']
        # if xlsxName:
        #     df_result.to_excel('output/{}.xlsx'.format(xlsxName), index=False)

        return df_result

    def gen_3(self, file_3, month, xlsxName=''):
        opMonth = pd.to_datetime(month + '.01', format='%Y.%m.%d').date()

        df_3 = pd.read_excel(file_3, sheet_name=month, skiprows=5, header=None)
        df_3 = df_3[df_3[0].notna()]
        df_3.fillna(0, inplace=True)

        df_3_header = pd.read_excel(file_3, sheet_name=month, header=None, nrows=4)
        str_dt = self.fullwidth_to_halfwidth(df_3_header.iloc[0, 21])

        m = re.match('(\d+)\D+(\d+)\D+(\d+)\D+', str_dt)
        publishDate_3 = pd.to_datetime(
            '-'.join(list(m.groups())), format='%Y-%m-%d').date()

        df_3_header.fillna(method='ffill', inplace=True)  # fill vertically for na
        df_3_header = df_3_header[1:]
        df_3_header.reset_index(drop=True, inplace=True)

        lst_dicts = []
        for ridx, row in df_3.iterrows():
            companyName = ''
            # print(row)
            companyName = row[2]

            str_bits = ''
            for i in range(3, 9):
                if row[i] == '○':
                    str_bits += '1'
                else:
                    str_bits += '0'

            for i in range(10, 22):
                d1 = {'OpMonth': opMonth, 'CompanyName': companyName, 'DemandType1': df_3_header.iloc[0, i],
                      'DemandType2': df_3_header.iloc[1, i], 'DemandMW': row[i], 'PublishDate': publishDate_3}
                # d1['CompanyType'] = str_bits
                # d1['DemandType'] = df_3_header.iloc[5, i]
                lst_dicts.append(d1)
        df_result = pd.DataFrame(lst_dicts)
        if df_result.shape[0] > 0:
            df_result = df_result[df_result['DemandType2'] != '計']

        # if xlsxName:
        #     df_result.to_excel('output/{}.xlsx'.format(xlsxName), index=False)

        return df_result

    def gen_all_12(self, file_1, file_2, outfile=''):
        xl = pd.ExcelFile(file_1)
        lst_months = list(filter(lambda n: re.match(r'^20.*\d+$', n), xl.sheet_names))
        lst_df_12 = []
        for m in lst_months:
            for det in range(3, 10):
                print(m, det)
                lst_df_12.append(self.gen_1_2(file_1, file_2, m, det))

        df_12 = pd.concat(lst_df_12)
        if outfile:
            df_12.to_csv(outfile, index=False)
        return df_12

    def gen_all_3(self, file_3):
        xl = pd.ExcelFile(file_3)
        lst_months = list(filter(lambda n: re.match(r'^20.*\d+$', n), xl.sheet_names))
        lst_df_3 = []
        for m in lst_months:
            print(m)
            lst_df_3.append(self.gen_3(file_3, m))

        df_3 = pd.concat(lst_df_3)
        # if outfile:
        #   df_3.to_csv(outfile, index=False)

        return df_3

    def save(self):
        # Define the connection parameters
        conn_params = {
            'dbname': self.database,
            'user': self.user,
            'host': self.host,
            'port': self.port,
            'password': self.password
        }

        # Connect to the database
        with psycopg2.connect(**conn_params) as conn:
            # Create a cursor
            with conn.cursor() as cursor:
                # Define the SQL statement for creating the table
                create_table_sql = """
                    CREATE TABLE IF NOT EXISTS admin (
                        OpMonth DATE,
                        CompanyNameLocal VARCHAR(30),
                        FuelType VARCHAR(30),
                        GenerationType VARCHAR(30),
                        NumberOfPlant FLOAT,
                        MaxOutputMW FLOAT,
                        GenMW FLOAT,
                        PublishDate DATE,
                        InsertedTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """

                # Create the table if it doesn't already exist
                cursor.execute(create_table_sql)
                conn.commit()

                # Download the data from the URL
                data = self.gen_all_12(file_1, file_2)

                # Add the current time as the insertedTime column
                data["InsertedTime"] = datetime.datetime.now()

                # Convert the DataFrame to a string in CSV format
                data_str = data.to_csv(index=False, header=False, na_rep='')

                # Pass the data string to the copy_from method
                try:
                    cursor.copy_from(io.StringIO(data_str), 'admin', sep=',', null='')
                except Exception as e:
                    conn.rollback()
                    print(f"Error inserting data: {e}")
                else:
                    conn.commit()

                # Execute a SELECT statement to retrieve the data from the table
                cursor.execute("SELECT * FROM admin")

                # Print the retrieved data
                for row in cursor:
                    print(row)

    def saveToDb(self):
        # Define the connection parameters
        conn_params = {
            'dbname': self.database,
            'user': self.user,
            'host': self.host,
            'port': self.port,
            'password': self.password
        }

        # Connect to the database
        with psycopg2.connect(**conn_params) as conn:
            # Create a cursor
            with conn.cursor() as cursor:
                # Define the SQL statement for creating the table
                create_table_sql = """
                    CREATE TABLE IF NOT EXISTS admin1(
                        OpMonth DATE,
                        CompanyName VARCHAR(100),
                        DemandType1 VARCHAR(100),
                        DemandType2 VARCHAR(100),
                        DemandMW FLOAT,
                        PublishDate DATE,
                        InsertedTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                # PRIMARY KEY(OpMonth, CompanyName, DemandType1, DemandType2)
                # Create the table if it doesn't already exist
                cursor.execute(create_table_sql)
                conn.commit()

                # Download the data from the URL
                data = self.gen_all_3(file_3)

                # Add the current time as the insertedTime column
                data["insertedTime"] = datetime.datetime.now()

                # Convert the DataFrame to a string in CSV format
                data_str = data.to_csv(index=False, header=False)

                # Pass the data string to the copy_from method
                try:
                    cursor.copy_from(io.StringIO(data_str), 'admin1', sep=',', columns=(
                        'opmonth', 'companyname', 'demandtype1', 'demandtype2', 'demandmw', 'publishdate',
                        'insertedtime'))
                except psycopg2.errors.UniqueViolation as e:
                    conn.rollback()
                    print(f"Skipping duplicate records: {e}")
                else:
                    conn.commit()

                # Execute a SELECT statement to retrieve the data from the table
                cursor.execute("SELECT * FROM admin1")

                # Print the retrieved data
                for row in cursor:
                    print(row)


file_1 = 'https://www.enecho.meti.go.jp/statistics/electric_power/ep002/xls/2022/1-1-2022n.xlsx'
file_2 = 'https://www.enecho.meti.go.jp/statistics/electric_power/ep002/xls/2022/2-1-2022n.xlsx'
file_3 = 'https://www.enecho.meti.go.jp/statistics/electric_power/ep002/xls/2022/3-1-2022n.xlsx'

sol = Solution()

# sol.save()
sol.saveToDb()
