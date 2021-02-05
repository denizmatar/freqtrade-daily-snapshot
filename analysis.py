import time
import sqlite3
import json
from datetime import date, timedelta
from binance.client import Client
from keys import *


class Analysis:

    SQL_COMMAND_ALL = "SELECT trades.id AS trades_id, trades.exchange AS trades_exchange, trades.pair AS trades_pair, trades.is_open AS trades_is_open, trades.fee_open AS trades_fee_open, trades.fee_open_cost AS trades_fee_open_cost, trades.fee_open_currency AS trades_fee_open_currency, trades.fee_close AS trades_fee_close, trades.fee_close_cost AS trades_fee_close_cost, trades.fee_close_currency AS trades_fee_close_currency, trades.open_rate AS trades_open_rate, trades.open_rate_requested AS trades_open_rate_requested, trades.open_trade_value AS trades_open_trade_value, trades.close_rate AS trades_close_rate, trades.close_rate_requested AS trades_close_rate_requested, trades.close_profit AS trades_close_profit, trades.close_profit_abs AS trades_close_profit_abs, trades.stake_amount AS trades_stake_amount, trades.amount AS trades_amount, trades.amount_requested AS trades_amount_requested, trades.open_date AS trades_open_date, trades.close_date AS trades_close_date, trades.open_order_id AS trades_open_order_id, trades.stop_loss AS trades_stop_loss, trades.stop_loss_pct AS trades_stop_loss_pct, trades.initial_stop_loss AS trades_initial_stop_loss, trades.initial_stop_loss_pct AS trades_initial_stop_loss_pct, trades.stoploss_order_id AS trades_stoploss_order_id, trades.stoploss_last_update AS trades_stoploss_last_update, trades.max_rate AS trades_max_rate, trades.min_rate AS trades_min_rate, trades.sell_reason AS trades_sell_reason, trades.sell_order_status AS trades_sell_order_status, trades.strategy AS trades_strategy, trades.timeframe AS trades_timeframe FROM trades"
    SQL_COMMAND_TOTAL_PROFIT = "SELECT trades.close_profit_abs FROM trades WHERE sell_order_status ='closed'"
    SQL_COMMAND_DAILY_PROFIT = "SELECT trades.id, trades.close_profit_abs FROM trades"
    SQL_COMMAND_PAIR_INFO = "SELECT trades.open_rate, trades.max_rate, trades.close_rate, trades.close_profit, trades.stake_amount, trades.pair " \
                            "FROM trades " \
                            "WHERE sell_order_status ='closed'"
    SQL_COMMAND_TIMESTAMP_OPEN = "ALTER TABLE trades ADD open_timestamp"
    SQL_COMMAND_TIMESTAMP_CLOSE = "ALTER TABLE trades ADD close_timestamp"
    SQL_COMMAND_MAX_OPEN_TRADES = "SELECT trades.pair, trades.open_timestamp, trades.close_timestamp FROM trades WHERE close_timestamp IS NOT NULL"
    SQL_COMMAND_DAILY_TRADE_COUNT = "SELECT trades.close_timestamp, trades.id FROM trades WHERE close_timestamp IS NOT NULL"
    SQL_COMMAND_TOTAL_TRADE_COUNT = "SELECT COUNT(*) FROM trades"
    SQL_COMMAND_TIMESTAMP_GENERATOR = "SELECT trades.open_date, trades.close_date, id FROM trades WHERE close_timestamp IS NULL"
    SQL_COMMAND_TIMESTAMP_GENERATOR_INSERT_OPEN = "UPDATE trades SET open_timestamp = ? WHERE id = ?"
    SQL_COMMAND_TIMESTAMP_GENERATOR_INSERT_CLOSE = "UPDATE trades SET close_timestamp = ? WHERE id = ?"

    INVESTORS = {
        "DENIZ": {"investment": 500},
        "BARAN": {"profit": 0.5,
                  "investment": 200}
    }

    PROFIT_DENIZ = 0.5
    PROFIT_BARAN = 0.5

    INITIAL_INVESTMENT_DENIZ = 500
    INITIAL_INVESTMENT_BARAN = 200

    STAKE_AMOUNT_V1 = 16.5
    STAKE_AMOUNT_V2 = 62.5
    STAKE_AMOUNT_V3 = 87.5

    WHOLE_DAY_TIMESTAMP = 86400
    DELTA_UTC = 10800


    def __init__(self, db_path="/Users/denizmatar/freqtrade/raspberry_pi/tradesv3.sqlite"):
        self.db_path = db_path
        self.db_connector()
        self.current_time, self.yesterday = self.get_date()
        self.current_timestamp = self.current_timestamp_generator()
        self.add_timestamp_columns()
        self.timestamp_generator()
        self.daily_id_list = self.daily_id_list_generator()
        self.daily_trade_number = self.daily_trade_counter()
        self.total_trade_number = self.total_trade_counter()
        self.daily_profit = self.daily_profit_calculator()
        self.total_profit = self.total_profit_calculator()
        # self.max_open_trades = self.max_open_trades_calculator() # this takes so long, disabled for now
        self.daily_investment = self.daily_investment_calculator()
        self.total_investment = self.total_investment_calculator()
        self.daily_roi = self.roi_calculator("daily")
        self.total_roi = self.roi_calculator("total")
        self.balance = self.get_balances()
        self.profit_deniz, self.profit_baran = self.profit_apportioner()
        self.data_dictionary = self.dictionary_builder()

    def daily_snapshot(self):
        '''Sort of the main function. Calls dictionary builder to build the dictionary, then writes the new values into csv'''
        import csv

        field_names = ["date", "account_balance", "daily_profit", "profit_baran", "profit_deniz", "daily_trade_count",
                       "stake_amount", "daily_investment", "daily_ROI", "total_investment", "total_ROI"] # , "max_open_trades"]

        data_dictionary = self.data_dictionary
        print("Data dictionary created.")
        print(data_dictionary)

        with open('daily_snapshots.csv', mode='a') as csv_file:
            print("Csv file opened.")
            writer = csv.DictWriter(csv_file, fieldnames=field_names, delimiter=',', extrasaction='ignore')
            # writer.writeheader()
            writer.writerow(data_dictionary)
            print("Row written")
        self.db_closer()

    def db_connector(self):
        '''Starts the connection with the database'''
        global connection, cursor
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        print("DB connection successful.")

    def db_closer(self):
        '''Closes the connection with the database'''
        cursor.close()
        connection.close()
        print("DB closed.")

    def get_date(self):
        '''Returns current date: (i.e 2021-02-04)'''
        t = time.localtime()

        # Only for yesterday
        today = date.today()
        yesterday = today - timedelta(1)
        print(str(yesterday))

        current_time = time.strftime("%Y-%m-%d", t)
        return current_time, str(yesterday)

    def current_timestamp_generator(self):
        current_timestamp = str(time.mktime(time.strptime(self.current_time, "%Y-%m-%d"))).split(".")[0]
        # current_timestamp = 1612310400        # for testing with old databases
        return current_timestamp

    def get_all(self):
        '''Gathers all data'''
        cursor.execute(self.SQL_COMMAND_ALL)
        results = cursor.fetchall()
        return results

    def daily_profit_calculator(self):
        '''Calculates the daily profit of the PREVIOUS DAY'''
        cursor.execute(self.SQL_COMMAND_DAILY_PROFIT)
        results = cursor.fetchall()  # type(results): list
        # print(results)

        daily_profit = 0

        for id, profit in results:
            if id in self.daily_id_list:
                daily_profit += profit
        return daily_profit

    def total_profit_calculator(self):
        '''Calculates the total profit'''
        cursor.execute(self.SQL_COMMAND_TOTAL_PROFIT)
        results = cursor.fetchall()  # type(results): list

        total_profit = 0

        for r in results:
            try:
                total_profit += r[0]  # type(r): tuple
            except:
                pass
        return total_profit

    def pair_info(self):
        '''Calculates and prints some values for all past trades'''
        cursor.execute(self.SQL_COMMAND_PAIR_INFO)
        results = cursor.fetchall()  # type(results): list

        for i in range(len(results)):
            fail = 0
            stake_amount = results[i][4]
            pair_name = results[i][5]
            open_value = results[i][0]
            max_value = results[i][1]
            close_value = results[i][2]
            close_profit = str(results[i][3] * 100)[:4]
            potential_profit = ((max_value - open_value) / open_value) * 100
            if max_value > close_value:
                print("Pair No:", i + 1, "\n"
                      "Pair Name:", pair_name, "\n"
                      "Open:", open_value, "\n"
                      "Max:", max_value, "\n"
                      "Close:", close_value, "\n"
                      "Close Profit(%):    ", close_profit, "\n"
                      "Potential Profit(%):", potential_profit, "\n")
                fail += 1
        print(len(results) - fail, "out of", len(results), "pairs were potentially more profitable.")

    def add_timestamp_columns(self):
        '''Adds timestamp columns to the database. Only works if there is none'''

        # Try adding open_timestamp column
        try:
            cursor.execute(self.SQL_COMMAND_TIMESTAMP_OPEN)
        except:
            pass

        # Try adding close_timestamp column
        try:
            cursor.execute(self.SQL_COMMAND_TIMESTAMP_CLOSE)
        except:
            pass

    def timestamp_generator(self):
        '''Generates and inserts the timestamps. Note that timestamps are adjusted according to UTC+3'''
        cursor.execute(self.SQL_COMMAND_TIMESTAMP_GENERATOR)
        results = cursor.fetchall()  # type(results): list

        for i in range(len(results)):
            try:
                open_date = results[i][0][:16] # [:16] means --> "%Y-%m-%d %H:%M
                close_date = results[i][1][:16]
                open_date_timestamp = str(time.mktime(time.strptime(open_date, "%Y-%m-%d %H:%M"))).split(".")[0]
                close_date_timestamp = str(time.mktime(time.strptime(close_date, "%Y-%m-%d %H:%M"))).split(".")[0]

                open_date_timestamp = int(open_date_timestamp)
                open_date_timestamp += self.DELTA_UTC
                close_date_timestamp = int(close_date_timestamp)
                close_date_timestamp += self.DELTA_UTC

                cursor.execute(self.SQL_COMMAND_TIMESTAMP_GENERATOR_INSERT_OPEN, (str(open_date_timestamp), results[i][2]))
                cursor.execute(self.SQL_COMMAND_TIMESTAMP_GENERATOR_INSERT_CLOSE, (str(close_date_timestamp), results[i][2]))
            except:
                pass
            connection.commit()

    def daily_id_list_generator(self):
        """Returns id list of the trades that are closed that day"""
        cursor.execute(self.SQL_COMMAND_DAILY_TRADE_COUNT)
        results = cursor.fetchall()

        daily_id_list = []

        current_timestamp = self.current_timestamp
        # yesterday_timestamp =
        previous_day_timestamp = int(current_timestamp) - self.WHOLE_DAY_TIMESTAMP + self.DELTA_UTC

        for timestamp in results:

            if previous_day_timestamp <= int(timestamp[0]) and int(timestamp[0]) < int(current_timestamp): # burda bi sikinti var
                daily_id_list.append(timestamp[1])
        print("Today's trade id's:", daily_id_list)
        return daily_id_list

    def daily_trade_counter(self):
        '''Returns the daily trade count'''
        return len(self.daily_id_list)

    def total_trade_counter(self):
        '''Returns the total trade count'''
        cursor.execute(self.SQL_COMMAND_TOTAL_TRADE_COUNT)
        results = cursor.fetchall()
        return results[0][0]

    def max_open_trades_calculator(self):
        """Returns max open trades for the whole DB"""
        cursor.execute(self.SQL_COMMAND_MAX_OPEN_TRADES)
        results = cursor.fetchall()  # type(results): list

        counts_list = []

        for i in range(int(results[0][1]), int(results[-1][2])):
            count = 0
            for pair, open_timestamp, close_timestamp in results:
                if i >= int(open_timestamp) and i <= int(close_timestamp):
                    count += 1
            if count not in counts_list:
                counts_list.append(count)
        return max(counts_list)

    def get_balances(self):
        '''Gets the current balance info using python-binance module. Note that shows in total current USDT value'''

        # Initiate the class
        account = Client(api_key=api_key, api_secret=secret_key)

        # GET account information
        account_dictionary = account.get_account()

        total_balance = 0

        pair_info_all = account.get_all_tickers()

        # Remove non-USDT pair info
        pair_info = [x for x in pair_info_all if "USDT" in x["symbol"][-4:]]

        for i in range(len(account_dictionary["balances"])):
            pair_name = account_dictionary['balances'][i]['asset']
            pair_name_usdt = f"{pair_name}USDT"
            if pair_name == "USDT":
                total_balance += float(account_dictionary['balances'][i]['free'])
            for j in range(len(pair_info)):
                if pair_name_usdt == pair_info[j]['symbol']:
                    total_balance += (float(account_dictionary["balances"][i]["free"]) + float(
                        account_dictionary["balances"][i]["locked"])) \
                                     * float(pair_info[j]['price'])
        return total_balance

    def daily_investment_calculator(self):
        '''Returns the amount invested previous day'''
        return len(self.daily_id_list) * self.STAKE_AMOUNT_V3

    def total_investment_calculator(self):
        '''Returns the total amount invested'''
        return self.total_trade_number * self.STAKE_AMOUNT_V3

    def roi_calculator(self, range):
        '''Returns either daily return on investment or total return on investment'''
        if range == "daily":
            try:
                daily_roi = self.daily_profit / self.daily_investment * 100
                return daily_roi
            except:
                return 0
        elif range == "total":
            try:
                total_roi = self.total_profit / self.total_investment * 100
                return total_roi
            except:
                return 0

    def profit_apportioner(self):
        '''Returns ratio of profit for the trader and investors'''
        investment_deniz = self.INVESTORS['DENIZ']['investment']
        investment_baran = self.INVESTORS['BARAN']['investment']
        comission_baran = self.INVESTORS['BARAN']['profit']

        total_investment = investment_deniz + investment_baran

        profit_share_ratio_deniz = (investment_deniz + investment_baran * comission_baran) / total_investment
        profit_share_ratio_baran = 1 - profit_share_ratio_deniz

        profit_deniz = profit_share_ratio_deniz * self.daily_profit
        profit_baran = profit_share_ratio_baran * self.daily_profit

        return profit_deniz, profit_baran

    def dictionary_builder(self):
        '''Builds and returns a dictionary to later transform into csv format'''
        data_dictionary = {
            "date": self.yesterday,
            "account_balance": self.balance,
            "daily_profit": self.daily_profit,
            "profit_baran": self.profit_baran,
            "profit_deniz": self.profit_deniz,
            "daily_trade_count": self.daily_trade_number,
            "stake_amount": self.STAKE_AMOUNT_V3,
            "daily_investment": self.daily_investment,
            "daily_ROI": self.daily_roi,
            "total_investment": self.total_investment,
            "total_ROI": self.total_roi
            # "max_open_trades:": self.max_open_trades,
        }
        return data_dictionary

    def mailer(self):
        '''E-mails daily_snapshot.csv'''

        import smtplib
        from email.message import EmailMessage


        password = mail_password
        smtp_server = "smtp.gmail.com"
        sender_email = "testerdeniztester1@gmail.com"
        receiver_email1 = "denizmatar1@gmail.com"
        receiver_email2 = "baranselcarpa@gmail.com"
        message = ""

        # server.starttls()

        msg = EmailMessage()
        msg['Subject'] = "{} Daily Snapshot".format(self.yesterday)
        msg['From'] = sender_email
        msg['To'] = receiver_email1, receiver_email2
        msg.set_content(message)

        attachment = '/Users/denizmatar/freqtrade/user_data/strategy_analysis/daily_snapshots.csv'

        with open(attachment, 'rb') as f:
            file_data = f.read()
            file_name = f.name
        msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

        server = smtplib.SMTP_SSL(smtp_server, 465)
        server.login(sender_email, password)
        print("Login Success")

        # msg['To'] = receiver_email2
        server.send_message(msg)
        print("Message Sent")