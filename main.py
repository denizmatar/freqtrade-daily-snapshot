from analysis import Analysis


db = Analysis()
db.daily_snapshot() # --> makes a snapshot from database and writes to a csv file
db.mailer()         # --> mails the csv file

