import time
import sqlite3
import json
import copy
from datetime import date, timedelta
from binance.client import Client
from config import *


class Analysis:

    CSV_PATH = csv_path

    DATABASE_PATH = database_path

    STAKE_AMOUNT_V1 = 16.5
    STAKE_AMOUNT_V2 = 62.5
    STAKE_AMOUNT_V3 = 87.5
    STAKE_AMOUNT_V4 = 140     # Only V4 is used

    SQL_COMMAND_ALL = "SELECT trades.id AS trades_id, trades.exchange AS trades_exchange, trades.pair AS trades_pair, trades.is_open AS trades_is_open, trades.fee_open AS trades_fee_open, trades.fee_open_cost AS trades_fee_open_cost, trades.fee_open_currency AS trades_fee_open_currency, trades.fee_close AS trades_fee_close, trades.fee_close_cost AS trades_fee_close_cost, trades.fee_close_currency AS trades_fee_close_currency, trades.open_rate AS trades_open_rate, trades.open_rate_requested AS trades_open_rate_requested, trades.open_trade_value AS trades_open_trade_value, trades.close_rate AS trades_close_rate, trades.close_rate_requested AS trades_close_rate_requested, trades.close_profit AS trades_close_profit, trades.close_profit_abs AS trades_close_profit_abs, trades.stake_amount AS trades_stake_amount, trades.amount AS trades_amount, trades.amount_requested AS trades_amount_requested, trades.open_date AS trades_open_date, trades.close_date AS trades_close_date, trades.open_order_id AS trades_open_order_id, trades.stop_loss AS trades_stop_loss, trades.stop_loss_pct AS trades_stop_loss_pct, trades.initial_stop_loss AS trades_initial_stop_loss, trades.initial_stop_loss_pct AS trades_initial_stop_loss_pct, trades.stoploss_order_id AS trades_stoploss_order_id, trades.stoploss_last_update AS trades_stoploss_last_update, trades.max_rate AS trades_max_rate, trades.min_rate AS trades_min_rate, trades.sell_reason AS trades_sell_reason, trades.sell_order_status AS trades_sell_order_status, trades.strategy AS trades_strategy, trades.timeframe AS trades_timeframe FROM trades"
    SQL_COMMAND_TOTAL_PROFIT = "SELECT trades.close_profit_abs FROM trades WHERE sell_order_status ='closed'"
    SQL_COMMAND_DAILY_PROFIT = "SELECT trades.id, trades.close_profit_abs FROM trades"
    SQL_COMMAND_PAIR_INFO = "SELECT trades.open_rate, trades.max_rate, trades.close_rate, trades.close_profit, trades.stake_amount, trades.pair " \
                            "FROM trades " \
                            "WHERE sell_order_status ='closed'"
    SQL_COMMAND_TIMESTAMP_OPEN = "ALTER TABLE trades ADD open_timestamp"
    SQL_COMMAND_TIMESTAMP_CLOSE = "ALTER TABLE trades ADD close_timestamp"
    SQL_COMMAND_MAX_OPEN_TRADES = "SELECT trades.pair, trades.open_timestamp, trades.close_timestamp FROM trades WHERE close_timestamp IS NOT NULL"
    SQL_COMMAND_TOTAL_INVESTMENT = "SELECT SUM(trades.stake_amount) FROM trades"
    SQL_COMMAND_DAILY_TRADE_COUNT = "SELECT trades.close_timestamp, trades.id FROM trades WHERE close_timestamp IS NOT NULL"
    SQL_COMMAND_TOTAL_TRADE_COUNT = "SELECT COUNT(*) FROM trades"
    SQL_COMMAND_TIMESTAMP_GENERATOR = "SELECT trades.open_date, trades.close_date, id FROM trades"#" WHERE close_timestamp IS NULL"
    SQL_COMMAND_TIMESTAMP_GENERATOR_INSERT_OPEN = "UPDATE trades SET open_timestamp = ? WHERE id = ?"
    SQL_COMMAND_TIMESTAMP_GENERATOR_INSERT_CLOSE = "UPDATE trades SET close_timestamp = ? WHERE id = ?"

    WHOLE_DAY_TIMESTAMP = 86400
    DELTA_UTC = 10800

    def __init__(self):
        self.db_path = self.DATABASE_PATH
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
        # self.max_open_trades = self.max_open_trades_calculator()              # this takes so long, disabled for now
        self.daily_investment = self.daily_investment_calculator()
        self.total_investment = self.total_investment_calculator()
        self.daily_roi = self.roi_calculator("daily")
        self.total_roi = self.roi_calculator("total")
        self.balance = self.get_balances()
        self.profit_apportioner()
        self.data_dictionary = self.dictionary_builder()

    def daily_snapshot(self, headers=False):
        '''Sort of the main function. Calls dictionary builder to build the dictionary, then writes the new values into csv'''

        for investor in INVESTORS:
            investor_name = INVESTORS[investor]['id']

            if investor == 0:
                data_dictionary = copy.deepcopy(self.data_dictionary)
                for investor in INVESTORS:
                    data_dictionary["profit_{}".format(INVESTORS[investor]['id'])] = \
                        self.float_formatter(INVESTORS[investor]['profit_ratio'] * self.daily_profit) + "$"

                print("Data dictionary created for {}".format(investor_name))
                field_names = list(data_dictionary.keys())

                self.csv_writer(investor_name, field_names, headers, data_dictionary)

            else:
                data_dictionary = copy.deepcopy(self.data_dictionary)
                data_dictionary["profit_{}".format(investor_name)] = \
                self.float_formatter(INVESTORS[investor]['profit_ratio'] * self.daily_profit) + "$"

                print("Data dictionary created for {}".format(investor_name))
                field_names = list(data_dictionary.keys())

                self.csv_writer(investor_name, field_names, headers, data_dictionary)

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

    def csv_writer(self, investor_name, field_names, headers, data_dictionary):
        import csv

        with open(self.CSV_PATH.format(investor_name), mode='a') as csv_file:
            print("{}'s daily_snapshot.csv file opened.".format(investor_name))
            writer = csv.DictWriter(csv_file, fieldnames=field_names, delimiter=',', extrasaction='ignore')
            if headers:
                writer.writeheader()
            writer.writerow(data_dictionary)
            print("New row added for {}".format(investor_name))

    def get_date(self):
        '''Returns current date: (i.e 2021-02-04)'''
        today = date.today()
        yesterday = today - timedelta(1)
        print("Date:", str(yesterday))

        return str(today), str(yesterday)

    def current_timestamp_generator(self):
        current_timestamp = int(str(time.mktime(time.strptime(self.current_time, "%Y-%m-%d"))).split(".")[0])
        # current_timestamp = 1612828800        # for testing with old databases
        return current_timestamp
    # TODO: add functionality by adding a time parameter

    def get_all(self):
        '''Gathers all data'''
        cursor.execute(self.SQL_COMMAND_ALL)
        results = cursor.fetchall()
        return results

    def daily_profit_calculator(self):
        '''Calculates the daily profit of the PREVIOUS DAY'''
        cursor.execute(self.SQL_COMMAND_DAILY_PROFIT)
        results = cursor.fetchall()

        daily_profit = 0

        for id, profit in results:
            if id in self.daily_id_list:
                daily_profit += profit
        return daily_profit

    def total_profit_calculator(self):
        '''Calculates the total profit'''
        cursor.execute(self.SQL_COMMAND_TOTAL_PROFIT)
        results = cursor.fetchall()

        total_profit = 0

        for r in results:
            try:
                total_profit += r[0]
            except:
                pass
        return total_profit

    def pair_info(self):
        '''Calculates and prints some values for all past trades'''
        cursor.execute(self.SQL_COMMAND_PAIR_INFO)
        results = cursor.fetchall()

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
        except sqlite3.OperationalError as e:
            print("open_timestamp column check okay. INFO:", e)
            pass

        # Try adding close_timestamp column
        try:
            cursor.execute(self.SQL_COMMAND_TIMESTAMP_CLOSE)
        except sqlite3.OperationalError as e:
            print("close_timestamp column check okay. INFO:", e)
            pass

    def timestamp_generator(self):
        '''Generates and inserts the timestamps. Note that timestamps are adjusted according to UTC+3'''
        cursor.execute(self.SQL_COMMAND_TIMESTAMP_GENERATOR)
        results = cursor.fetchall()

        for i in range(len(results)):
            try:
                open_date = results[i][0][:16] # [:16] means --> "%Y-%m-%d %H:%M
                close_date = results[i][1][:16]

                open_date_timestamp = str(time.mktime(time.strptime(open_date, "%Y-%m-%d %H:%M"))).split(".")[0]
                close_date_timestamp = str(time.mktime(time.strptime(close_date, "%Y-%m-%d %H:%M"))).split(".")[0]

                open_date_timestamp = int(open_date_timestamp) + self.DELTA_UTC
                close_date_timestamp = int(close_date_timestamp) + self.DELTA_UTC

                cursor.execute(self.SQL_COMMAND_TIMESTAMP_GENERATOR_INSERT_OPEN, (str(open_date_timestamp), results[i][2]))
                cursor.execute(self.SQL_COMMAND_TIMESTAMP_GENERATOR_INSERT_CLOSE, (str(close_date_timestamp), results[i][2]))
            except TypeError:
                print("Did not add timestamps for an unclosed trade (Trade ID: {})".format(results[i][2]))
            connection.commit()

    def daily_id_list_generator(self):
        """Returns id list of the trades that are closed that day"""
        cursor.execute(self.SQL_COMMAND_DAILY_TRADE_COUNT)
        results = cursor.fetchall()

        daily_id_list = []

        current_timestamp = self.current_timestamp #- self.DELTA_UTC # only substract for mac
        previous_day_timestamp = int(current_timestamp) - self.WHOLE_DAY_TIMESTAMP

        for timestamp, id in results:

            if previous_day_timestamp <= int(timestamp) and int(timestamp) < int(current_timestamp): # burda bi sikinti var
                daily_id_list.append(id)
        print("{} Trade IDs:".format(self.yesterday), daily_id_list)
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
    # FIXME: takes 60 seconds...

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
        return len(self.daily_id_list) * self.STAKE_AMOUNT_V4

    def total_investment_calculator(self):
        '''Returns the total amount invested'''
        cursor.execute(self.SQL_COMMAND_TOTAL_INVESTMENT)
        result = cursor.fetchall()
        return result[0][0]

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
        '''Returns ratio of profit for the investors. Works for any number of investors.
        Only note that the if statement in the for loop should be adjusted according to the trader's name'''
        investors_list = list(INVESTORS.values())
        number_of_investors = len(INVESTORS)
        total_investment = sum([investor['investment'] for investor in investors_list])
        investors_list[0]['profit_ratio'] = 1

        for i in range(number_of_investors):
            if investors_list[i]['id'] != 'DENIZ':
                profit_ratio = investors_list[i]["investment"] / total_investment * (1 - investors_list[i]["commission"])
                INVESTORS[0]['profit_ratio'] -= profit_ratio
                INVESTORS[i]["profit_ratio"] = profit_ratio

    def float_formatter(self, float):
        '''Formats the floats to display only 2 decimals'''
        return "{:.2f}".format(float)

    def dictionary_builder(self):
        '''Builds and returns a dictionary to later transform into csv format'''
        data_dictionary = {
            "date": self.yesterday,
            "account_balance": self.float_formatter(self.balance) + "$",
            "daily_profit": self.float_formatter(self.daily_profit) + "$",
            "daily_trade_count": self.daily_trade_number,
            "stake_amount": str(self.STAKE_AMOUNT_V4) + "$",
            "daily_investment": str(self.daily_investment) + "$",
            "daily_ROI": self.float_formatter(self.daily_roi) + "%",
            "total_investment": self.float_formatter(self.total_investment) + "$",
            "total_ROI": self.float_formatter(self.total_roi) + "%"
            # "max_open_trades:": self.max_open_trades,
        }

        return data_dictionary

    def mailer(self):
        '''E-mails daily_snapshot_{investor_name}.csv'''

        import smtplib
        from email.message import EmailMessage


        sender_email = "testerdeniztester1@gmail.com"
        password = mail_password
        smtp_server = "smtp.gmail.com"
        server = smtplib.SMTP_SSL(smtp_server, 465)
        server.login(sender_email, password)
        print("Login successful.")

        # server.starttls()     # don't know why this is here

        for investor in INVESTORS:
            investor_name = INVESTORS[investor]['id']

            attachment = self.CSV_PATH.format(investor_name)
            message = ""

            msg = EmailMessage()
            msg['Subject'] = f"Daily Snapshot of {self.yesterday}"
            msg['From'] = "Deniz MATAR <testerdeniztester1@gmail.com>"
            msg['To'] = INVESTORS[investor]['email']
            msg.set_content(message)

            with open(attachment, 'rb') as f:
                file_data = f.read()
                file_name = f.name
            msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

            server.send_message(msg)
            print(f"Message sent to {investor_name}")
