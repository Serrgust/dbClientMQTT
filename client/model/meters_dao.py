from .main_dao import MainDAO
import urllib.request
import urllib.error
import urllib.parse
import json
import pprint
from config.api_key import API_KEY


class MetersDAO(MainDAO):
    def __init__(self):
        MainDAO.__init__(self)

    def insert_kwh(self, meter, date, time, kwh_tot, good):
        cursor = self.conn.cursor()
        query = 'insert into "kWhTotal" ("Meter", "Date", "Time", "kWh_Tot", "Good")' \
                'values (%s, %s, %s, %s, %s) returning "Meter", "Date", "Time", "kWh_Tot", "Good";'
        cursor.execute(query, (meter, date, time, kwh_tot, good))
        row = cursor.fetchone()
        self.conn.commit()
        return row

    def insert_kw(self, meter, date, time, kw, good):
        cursor = self.conn.cursor()
        query = 'insert into "kW" ("Meter", "Date", "Time", "Watts", "Good")' \
                'values (%s, %s, %s, %s, %s) returning "Meter", "Date", "Time", "Watts", "Good";'
        cursor.execute(query, (meter, date, time, kw, good))
        row = cursor.fetchone()
        self.conn.commit()
        return row

    def verify_kwh_meter_date_time_exists(self, meter, date, time):
        cursor = self.conn.cursor()
        query = 'select "Meter" from "kWhTotal" where "Meter" = %s and "Date" = %s and "Time" = %s;'
        cursor.execute(query, (meter, date, time))
        row = cursor.fetchone()
        if not row:
            return False
        else:
            return True

    def verify_kw_meter_date_time_already_exists(self, meter, date, time):
        cursor = self.conn.cursor()
        query = 'select "Meter" from "kW" where "Meter" = %s and "Date" = %s and "Time" = %s;'
        cursor.execute(query, (meter, date, time))
        row = cursor.fetchone()
        if not row:
            return False
        else:
            return True

    def retrieve_meter_kwh_by_date(self, meter, start_date, end_date):
        cursor = self.conn.cursor()
        if end_date is None:
            query = 'select * from "kWhTotal" where "Meter" = %s and "Date" >= %s'
            cursor.execute(query, (meter, start_date))
        else:
            query = 'select * from "kWhTotal" where "Meter" = %s and "Date" between %s and %s'
            cursor.execute(query, (meter, start_date, end_date))
        row = cursor.fetchall()
        self.conn.commit()
        return row

    def retrieve_meter_kwh_by_week(self, meter, week_date):
        cursor = self.conn.cursor()
        query = """
        SELECT "Date", MAX("kWh_Tot") FROM "kWhTotal" 
        WHERE "Meter" = %s AND "Date" in (""" + week_date + """) group by "Date" ORDER BY "Date"
        """

        # query = 'select "Date", max("kWh_Tot") from "kWhTotal" where "Meter" = %s and' \
        #         ' "Date" in %s' \
        #         ' group by "Date", "kWh_Tot" order by "Date"'
        cursor.execute(query, (meter,))
        row = cursor.fetchall()
        self.conn.commit()
        return row

    def retrieve_meter_kw_day(self, meter, day):
        cursor = self.conn.cursor()
        query = 'select * from "kW" where "Meter" = %s and "Date" = %s'
        cursor.execute(query, (meter, day,))
        row = cursor.fetchall()
        self.conn.commit()
        return row
