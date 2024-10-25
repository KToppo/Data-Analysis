from SQLServerOP import MySQL_Server
from nselib import capital_market
import pandas as pd
# import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

class update_stock_data():

    def __init__(self):
        # Initialize the SQL server connection
        self.SQL_Server = MySQL_Server()
        connection = self.SQL_Server.create_server_connecter()

        # Create database 'Share_Mega_Data' if it doesn't exist
        query = "create database if not exists Share_Mega_Data"
        self.SQL_Server.create_database(connection=connection, query=query)

        # Establish connection to the 'Share_Mega_Data' database
        self.connection = self.SQL_Server.create_db_connection("Share_Mega_Data")


    def date_of_opp(self):
        # Create 'tables_data' table if it doesn't exist
        query = '''create table if not exists tables_data(
                    Table_ID int primary key,
                    Table_Name varchar(50),
                    Update_Date varchar(15)
                    );'''
        self.SQL_Server.execute_query(connection=self.connection, query=query)


    def equity_list_update(self):
        # Query to check if the 'equity_list' table exists and fetch its update date
        query = '''select Table_Name, Update_Date from tables_data where Table_Name=\'equity_list\''''
        condition = self.SQL_Server.read_query(connection=self.connection, query=query)


        if condition == []:
            # Create 'equity_list' table if it doesn't exist
            query = '''create table if not exists equity_list(
                        Date_Of_Listing varchar(15),
                        Symbol varchar(50) primary key,
                        Name_Of_Company varchar(100),
                        Face_Value int
                        );'''
            self.SQL_Server.execute_query(connection=self.connection, query=query)
            # Insert metadata into 'tables_data'
            query = '''insert into tables_data(Table_ID, Table_Name, Update_Date) values (001, \'equity_list\', \'NE\')'''
            self.SQL_Server.execute_query(connection=self.connection, query=query)

        elif ("equity_list" in condition[0]) and (date.today().strftime('%d_%m_%Y') not in condition[0]):
            # If the table exists and needs updating
            query = "Select Symbol from equity_list;"
            existing_data = self.SQL_Server.read_query(connection=self.connection, query=query)
            existing_data = pd.DataFrame(existing_data, columns=['symbol'])
            existing_data = list(existing_data['symbol'].values)

            # Get the latest equity list
            row_data = capital_market.equity_list()
            new_data = row_data.groupby(" SERIES").get_group("EQ")
            new_data = list(new_data["SYMBOL"].values)

            if existing_data == new_data:
                # If the existing data matches the new data, just update the update date
                query = f'''UPDATE tables_data SET Update_Date=\'{date.today().strftime('%d_%m_%Y')}\' WHERE Table_Name=\'equity_list\';'''
                self.SQL_Server.execute_query(connection=self.connection, query=query)
                return
            else:
                # If the stock list needs to be updated
                print("Stock list is needed to be updated")
                query = '''insert into tables_data(Table_ID, Table_Name, Update_Date) values (001, \'equity_list\', \'NE\')'''
                self.SQL_Server.execute_query(connection=self.connection, query=query)

        elif ("equity_list" in condition[0]) and (date.today().strftime('%d_%m_%Y') in condition[0]):
            return


        # Retriving, cleaning and filturing the Listed shares List
        dates = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
                "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
                "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
                "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}
        row_data = capital_market.equity_list()
        groping_by_series = row_data.groupby(" SERIES")
        EQ_List = groping_by_series.get_group("EQ")
        filtered_data = EQ_List[[" DATE OF LISTING", "SYMBOL", "NAME OF COMPANY", " FACE VALUE"]]

        for key in dates.keys():
            filtered_data = filtered_data.replace({" DATE OF LISTING":key}, {" DATE OF LISTING":dates[key]}, regex=True)

        # updating database
        tupled_data = tuple(tuple(IND_data) for IND_data in filtered_data.values.tolist())
        query = "insert into equity_list values "
        commas = 0
        for data in tupled_data:
            query += str(data)
            commas += 1
            if commas < len(tupled_data):
                query += ","
        query += ";"
        self.SQL_Server.execute_query(connection=self.connection, query=query)

        # Update the update date for the 'equity_list' table
        query = f'''UPDATE tables_data SET Update_Date=\'{date.today().strftime('%d_%m_%Y')}\' WHERE Table_Name=\'equity_list\';'''
        self.SQL_Server.execute_query(connection=self.connection, query=query)


    def price_volume_and_deliverable_position_list_update(self):
        # Query to check if the table 'stock_price_volume_deliverable' exists and fetch its update date
        query = '''select Table_Name, Update_Date from tables_data where Table_Name=\'stock_price_volume_deliverable\''''
        condition = self.SQL_Server.read_query(connection=self.connection, query=query)


        if condition == []:
            # Create 'stock_price_volume_deliverable' table if it doesn't exist
            query = '''create table if not exists stock_price_volume_deliverable(
                        Symbol varchar(50),
                        Date varchar(15),
                        Prev_Close decimal(15,2),
                        Open_Price decimal(15,2),
                        High_Price decimal(15,2),
                        Low_Price decimal(15,2),
                        Last_Price decimal(15,2),
                        Close_Price decimal(15,2),
                        Average_Price decimal(15,2),
                        Total_Traded_Quantity decimal(15,2),
                        Turnover decimal(15,2),
                        No_of_Trades decimal(15,2),
                        Deliverable_QTY decimal(15,2),
                        Dly_QT_to_Traded_QTY decimal(15,2),
                        PRIMARY KEY (Date, Symbol),
                        foreign key (Symbol) references equity_list(Symbol)
                        );'''
            self.SQL_Server.execute_query(connection=self.connection, query=query)

            # Update metadata into 'tables_data'
            query = '''insert into tables_data(Table_ID, Table_Name, Update_Date) values (002, \'stock_price_volume_deliverable\', \'NE\')'''
            self.SQL_Server.execute_query(connection=self.connection, query=query)


        elif ("stock_price_volume_deliverable" in condition[0]) and ("NE" in condition[0]):
            # If the table exists but has not been updated
            query = '''create table if not exists stock_price_volume_deliverable(
                        Symbol varchar(50),
                        Date varchar(15),
                        Prev_Close decimal(15,2),
                        Open_Price decimal(15,2),
                        High_Price decimal(15,2),
                        Low_Price decimal(15,2),
                        Last_Price decimal(15,2),
                        Close_Price decimal(15,2),
                        Average_Price decimal(15,2),
                        Total_Traded_Quantity decimal(15,2),
                        Turnover decimal(15,2),
                        No_of_Trades decimal(15,2),
                        Deliverable_QTY decimal(15,2),
                        Dly_QT_to_Traded_QTY decimal(15,2),
                        PRIMARY KEY (Date, Symbol),
                        foreign key (Symbol) references equity_list(Symbol)
                        );'''
            self.SQL_Server.execute_query(connection=self.connection, query=query)


        elif ("stock_price_volume_deliverable" in condition[0]) and (date.today().strftime('%d_%m_%Y') in condition[0]):
            # If table is updated
            return
        

        # Fetch the list of companies from 'equity_list'
        query = '''select Symbol, Date_Of_Listing from equity_list'''
        companislist = self.SQL_Server.read_query(connection=self.connection, query=query)
        total = len(companislist)
        with tqdm(total=total) as pbar:
            for share_name, listed_date in companislist:
                from_date = listed_date
                try:
                    # Get the last trading date of each company updated in database
                    query = f'''select Date from stock_price_volume_deliverable where Symbol=\'{share_name}\''''
                    trading_date = self.SQL_Server.read_query(connection=self.connection, query=query)
                    df = pd.DataFrame(trading_date,columns=["Dates"])
                    df["Dates"] = pd.to_datetime(df["Dates"], dayfirst=True)
                    df = df.sort_values(by="Dates", ascending=False)
                    df["Dates"] = df["Dates"] + pd.Timedelta('1 day')
                    df["Dates"] = df["Dates"].dt.strftime('%d-%m-%Y')
                    from_date = df.values[0][0]
                except:
                    pass
                if (date.today().strftime("%d-%m-%Y") == from_date) or (from_date == (date.today() + relativedelta(days=1)).strftime("%d-%m-%Y")):
                    # compairs the date with today to ensure the day gap of leat 2 day before update
                    pbar.update()
                    continue


                # Get price, volume and deliverable position data
                price_and_deliverable = capital_market.price_volume_and_deliverable_position_data(share_name,from_date=from_date, to_date=date.today().strftime("%d-%m-%Y"))
                if (list(price_and_deliverable.values) == []):
                    pbar.update()
                    continue


                # Data menuplation
                price_and_deliverable = price_and_deliverable.groupby('Series')
                try:
                    price_and_deliverable = price_and_deliverable.get_group("EQ") # Grouping Eqity based shares only
                except:
                    continue
                heads = ['PrevClose', 'OpenPrice', 'HighPrice',
                        'LowPrice', 'LastPrice', 'ClosePrice', 'AveragePrice',
                        'TotalTradedQuantity', 'TurnoverInRs', 'No.ofTrades', 'DeliverableQty',
                        '%DlyQttoTradedQty']
                dates = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
                        "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
                        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
                        "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}
                for head in heads:
                    price_and_deliverable[head] = price_and_deliverable[head].replace("-",0,regex=True).replace(",","",regex=True).astype("Float64")
                for key in dates.keys():
                    price_and_deliverable['Date'] = price_and_deliverable['Date'].replace(key, dates[key], regex=True)
                price_and_deliverable['S_Symbol'] = share_name
                filtered_data = price_and_deliverable[['S_Symbol', 'Date', 'PrevClose', 'OpenPrice', 'HighPrice', 'LowPrice', 'LastPrice', 'ClosePrice', 'AveragePrice', 'TotalTradedQuantity', 'TurnoverInRs', 'No.ofTrades','DeliverableQty', '%DlyQttoTradedQty']]
                filtered_data = filtered_data.drop_duplicates(subset=["Date", "S_Symbol"])


                # updating database
                tupled_data = tuple(tuple(IND_data) for IND_data in filtered_data.values.tolist())
                query = "insert into stock_price_volume_deliverable values "
                commas = 0
                for data in tupled_data:
                    query += str(data)
                    commas += 1
                    if commas < len(tupled_data):
                        query += ","
                query += ";"
                self.SQL_Server.execute_query(connection=self.connection, query=query)
                pbar.update()


        # Update the update date for the 'stock_price_volume_deliverable' table
        query = f'''UPDATE tables_data SET Update_Date=\'{date.today().strftime('%d_%m_%Y')}\' WHERE Table_Name=\'stock_price_volume_deliverable\';'''
        self.SQL_Server.execute_query(connection=self.connection, query=query)


    def bulk_block_deal_list_update(self):
        # Query to check if the 'bulk_block_deal' table exists and fetch its update date
        query = '''select Table_Name, Update_Date from tables_data where Table_Name=\'bulk_block_deal\''''
        condition = self.SQL_Server.read_query(connection=self.connection, query=query)


        if condition == []:
            # Create 'bulk_block_deal' table if it doesn't exist
            query = '''create table if not exists bulk_block_deal(
                        Symbol varchar(50),
                        Date varchar(15),
                        Client_Name varchar(150),
                        Buy_Sell varchar(5),
                        Quantity_Traded int(8),
                        Avg_Price decimal(10,2),
                        Type varchar(6),
                        foreign key (Symbol) references equity_list(Symbol)
                        );'''
            self.SQL_Server.execute_query(connection=self.connection, query=query)

            # Insert metadata into 'tables_data'
            query = '''insert into tables_data(Table_ID, Table_Name, Update_Date) values (003, \'bulk_block_deal\', \'NE\')'''
            self.SQL_Server.execute_query(connection=self.connection, query=query)

        elif ("bulk_block_deal" in condition[0]) and ("NE" in condition[0]):
            # If the table exists but has not been updated
            query = '''create table if not exists bulk_block_deal(
                        Symbol varchar(50),
                        Date varchar(15),
                        Client_Name varchar(150),
                        Buy_Sell varchar(5),
                        Quantity_Traded int(8),
                        Avg_Price decimal(10,2),
                        Type varchar(6),
                        foreign key (Symbol) references equity_list(Symbol)
                        );'''
            self.SQL_Server.execute_query(connection=self.connection, query=query)

        elif ("bulk_block_deal" in condition[0]) and (date.today().strftime('%d_%m_%Y') in condition[0]):
            return
        

        query = "select Date from bulk_block_deal"
        data_check = self.SQL_Server.read_query(connection=self.connection, query=query)

        if data_check == []:
            # Fetch the listing dates of companies from 'equity_list' if there are no existing dates
            query = '''select Date_Of_Listing from equity_list'''
            companislist = self.SQL_Server.read_query(connection=self.connection, query=query)
            companislist = pd.DataFrame(companislist, columns=["dates"])
            dates = companislist["dates"]
            dates = pd.to_datetime(dates, dayfirst=True)
            dates = dates.sort_values()
            dates = dates.dt.strftime('%d-%m-%Y')
            from_date = dates.values[0]

        else:
            # Updating pre-existing Data
            data_check = pd.DataFrame(data_check, columns=["dates"])
            dates = data_check["dates"].drop_duplicates()
            for d in dates:
                if (len(d) < 10):
                    dates = dates.drop(dates[dates==d].index)
            dates = pd.to_datetime(dates, dayfirst=True)
            dates = dates.sort_values(ascending=False)
            dates = dates.dt.strftime('%d-%m-%Y')
            from_date = dates.values[0]

        if (from_date == date.today().strftime('%d-%m-%Y')):
            # Skip the update if the data is already up to date
            return
        
         # Fetch and update block deals data
        bulk_deal = capital_market.bulk_deal_data(from_date=from_date, to_date=date.today().strftime('%d-%m-%Y'))
        self.bulk_and_block_filturar_update(bulk_deal, "Bulk")

        block_deal = capital_market.block_deals_data(from_date=from_date, to_date=date.today().strftime('%d-%m-%Y'))
        self.bulk_and_block_filturar_update(block_deal, "Block")

        # Update the updated date for the 'bulk_block_deal' table
        query = f'''UPDATE tables_data SET Update_Date=\'{date.today().strftime('%d_%m_%Y')}\' WHERE Table_Name=\'bulk_block_deal\';'''
        self.SQL_Server.execute_query(connection=self.connection, query=query)

    def bulk_and_block_filturar_update(self, deal, type):
        # Replace month abbreviations in the 'Date' column with numerical values
        dates = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
                "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
                "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
                "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}
        for key in dates.keys():
            deal['Date'] = deal['Date'].replace(key, dates[key], regex=True)

        
        # Query to get the list of symbols from 'equity_list'
        query = "select Symbol from equity_list;"
        EQ_symbol = self.SQL_Server.read_query(connection=self.connection, query=query)
        EQ_symbol = pd.DataFrame(EQ_symbol)


        # Filter out deals with symbols not in the equity list
        bulk_deal_group = deal.groupby('Symbol')
        bulk_deal_gropu_key = bulk_deal_group.groups.keys()
        for key_v in bulk_deal_gropu_key:
            if key_v not in EQ_symbol.values:
                deal = deal.drop(deal[deal['Symbol']==key_v].index)

        
        # Add the type of deal (Bulk/Block) to the dataframe
        deal['type'] = type


        # Prepare the data for insertion
        filtered_data = deal[['Symbol', 'Date', 'ClientName', 'Buy/Sell','QuantityTraded', 'TradePrice/Wght.Avg.Price', 'type']]
        filtered_data['QuantityTraded'] = filtered_data['QuantityTraded'].replace(',',"", regex=True).astype('Int64')
        filtered_data['TradePrice/Wght.Avg.Price'] = filtered_data['TradePrice/Wght.Avg.Price'].replace(',',"", regex=True).astype('Float64')
        tupled_data = tuple(tuple(IND_data) for IND_data in filtered_data.values.tolist())


        # Insert data into 'bulk_block_deal' table
        query = "insert into bulk_block_deal values "
        commas = 0
        for data in tupled_data:
            query += str(data)
            commas += 1
            if commas < len(tupled_data):
                query += ","
        query += ";"
        self.SQL_Server.execute_query(connection=self.connection, query=query)

        

# Initialize the updater class
updater = update_stock_data()

# Create and update the necessary tables
updater.date_of_opp()
updater.equity_list_update()
updater.price_volume_and_deliverable_position_list_update()
updater.bulk_block_deal_list_update()