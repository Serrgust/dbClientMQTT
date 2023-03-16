from client.model.meters_dao import MetersDAO


class Meters:

    def build_temp_dict(self, readings):
        return {
            'Temp': readings['main']['temp'],
            'Humidity': readings['main']['humidity'],
            'Clouds': readings['clouds']['all'],
            'City': readings['name'],
            'Country': readings['sys']['country'],
            'Weather Condition': readings['weather'][0]['main'] + ' -' + readings['weather'][0]['description'],
        }

    def build_insert_kwh_dict(self, readings):
        new_dict = {
            'Meter': int(readings['Meter']),
            'Time': readings['ReadData'][0]['Time'],
            'Date': readings['ReadData'][0]['Date'],
            'Time_Stamp_UTC_ms': readings['ReadData'][0]['Time_Stamp_UTC_ms'],
            'kWh_Tot': float(),
            'RMS_Watts_Tot': int(),
            'Good': int(readings['ReadData'][0]['Good'])
        }
        if 'kWh_Tot' in readings['ReadData'][0]:
            new_dict.update({'kWh_Tot': float(readings['ReadData'][0]['kWh_Tot'])}),
            new_dict.update({'RMS_Watts_Tot': int(readings['ReadData'][0]['RMS_Watts_Tot'])})
        return new_dict

    # def insert_kwh(self, received_json):
    #     dao = MetersDAO()
    #     Meter = str(received_json['Meter'])
    #     Date = str(received_json['Date'])
    #     kWh_Tot = str(received_json['kWh_Tot'])
    #     Good = str(received_json['Good'])
    #     if not dao.verify_kwh_meter_date_time_exists(Meter, Date, Time):
    #         row = dao.insert_kwh(Meter, Date, Time, kWh_Tot, Good)
    #         print("Inserted kWh of Meter: " + row[0])
    #     elif dao.verify_kwh_meter_date_time_exists(Meter, Date, Time):
    #         print("Meter: " + Meter + " kWh has not updated")
    #     return
    #
    # def insert_kw(self, received_json):
    #     dao = MetersDAO()
    #     Meter = str(received_json['Meter'])
    #     Date = str(received_json['Date'])
    #     Time = str(received_json['Time'])
    #     Watts = str(received_json['RMS_Watts_Tot'])
    #     Good = str(received_json['Good'])
    #     if not dao.verify_kw_meter_date_time_already_exists(Meter, Date, Time):
    #         row = dao.insert_kw(Meter, Date, Time, Watts, Good)
    #         print("Inserted kW of Meter: " + row[0])
    #     else:
    #         print("Meter: " + Meter + " kW has not updated")
    #     return

    def insert_meter(self, received_json):
        dao = MetersDAO()
        print(received_json)
        Meter = str(received_json['Meter'])
        Date = str(received_json['Date'])
        Time = str(received_json['Time'])
        if received_json.get('RMS_Watts_Tot'):
            Watts = str(received_json['RMS_Watts_Tot'])
        else:
            Watts = "0"
        kWh_Tot = str(received_json['kWh_Tot'])
        Good = str(received_json['Good'])
        if Good == "1" and not dao.verify_kw_meter_date_time_already_exists(Meter, Date, Time,
                                                                            Watts) and not dao.verify_kwh_meter_date_time_exists(
            Meter, Date, Time, kWh_Tot):
            row = dao.insert_kw(Meter, Date, Time, Watts, Good)
            print("Inserted kW of Meter: " + row[0])
            # if not dao.verify_kwh_meter_date_time_exists(Meter, Date, Time, kWh_Tot):
            row = dao.insert_kwh(Meter, Date, Time, kWh_Tot, Good)
            print("Inserted kWh of Meter: " + row[0])
        else:
            print("Meter: " + Meter + " has not updated")
        # elif dao.verify_kwh_meter_date_time_exists(Meter, Date, Time):
        #    print("Meter: " + Meter + " kWh has not updated")
        return

    def insert_temp(self, received_json):
        dao = MetersDAO()
        # Temp = str(received_json['Temp'])
        # Humidity = str(received_json['Humidity'])
        # City = str(received_json['City'])
        print(received_json)
        #  temp_dict = self.build_temp_dict(received_json)
        row = dao.insert_temp(received_json.get("Temp"), received_json.get("Humidity"), received_json.get("Clouds"),
                              received_json.get("City"), )  # received_json.get("Weather Condition")
        print("Inserted Temp: " + str(row[0]) + " of City: " + str(row[3]))
        return

    def retrieve_meter_kwh_by_date(self, received_json):
        dao = MetersDAO()
        print(received_json)
        meter = received_json["Meter"]
        start_date = received_json["Start"]
        a = start_date.split(" ")
        retrieved_list = []
        try:
            if received_json["End"] is not None:
                end_date = received_json["End"]
                b = end_date.split(" ")
                retrieved_list = dao.retrieve_meter_kwh_by_date(meter, a[0], b[0])
        except:
            None
        result_list = []
        for x in retrieved_list:
            new_dict = {
                "Meter": x[0],
                "Date": x[1],
                "Time": x[2],
                "kWh_Tot": x[3]
            }
            result_list.append(new_dict)
        print(result_list)
        return result_list

    # def retrieve_meterkwh_by_week(self, received_str):
    #     dao = MetersDAO()
    #     x = received_str.split(",")
    #     a = x[0]
    #     print(a)
    #     print(x)
    #     b = x.pop(0)
    #     print(x)
    #     c = ",".join(x)
    #     print(c)
    #     print(type(c))
    #     retrieved_list = dao.retrieve_meterkwh_by_week(a, c)
    #     result_list = []
    #     for x in retrieved_list:
    #         new_dict = {
    #             "Meter": x[0],
    #             "Date": x[1],
    #             "Time": x[2],
    #             "kWh_Tot": x[3]
    #         }
    #         result_list.append(new_dict)
    #     print(result_list)
    #     return result_list

    def retrieve_meter_kw_day(self, meter, day):
        dao = MetersDAO()
        retrieved_list = dao.retrieve_meter_kw_day(meter, day)
        avg_temp = dao.retrieve_temp_by_day(meter, day)
        print(type(avg_temp))
        print(len(avg_temp))
        result_list = []
        if len(avg_temp) != 0 and avg_temp[0][0] is not None and avg_temp[0][2] is not None:
            temp_dict = {
                "Avg Temp": float(avg_temp[0][0]),
                "Avg Hum": float(avg_temp[0][1]),
                "Avg Clouds": float(avg_temp[0][2])
            }
            result_list = [temp_dict]
        print(retrieved_list)
        for x in retrieved_list:
            new_dict = {
                "Meter": x[0],
                "Date": x[1],
                "Time": x[2],
                "Watts": x[3]
            }
            result_list.append(new_dict)
        print(result_list)
        return result_list

    def retrieve_meter_kwh_by_week(self, meter, days):
        dao = MetersDAO()
        retrieved_list = dao.retrieve_meter_kwh_by_week(meter, days)
        result_list = []
        # print(retrieved_list)
        avg_temp = dao.retrieve_temp_by_week(meter, days)
        print(type(avg_temp))
        print(len(avg_temp))
        result_list = []
        if len(avg_temp) != 0 and avg_temp[0][0] is not None and avg_temp[0][2] is not None:
            temp_dict = {
                "Avg Temp": float(avg_temp[0][0]),
                "Avg Hum": float(avg_temp[0][1]),
                "Avg Clouds": float(avg_temp[0][2])
            }
            result_list = [temp_dict]
        for x in retrieved_list:
            new_dict = {
                "Date": x[0],
                "kWh_Tot": x[1],
            }
            result_list.append(new_dict)
        print(result_list)
        return result_list

    def retrieve_meter_info(self, received_str):
        x = received_str.split(",")
        meter = x[0]
        if len(x) == 2:
            day = x[1]
            print(meter + " Day: " + day)
            result_list = self.retrieve_meter_kw_day(meter, day)
            return result_list
        else:
            x.pop(0)
            days = " "
            new_days = days.join(x)
            #  print(meter + " Days: " + new_days)
            new_days = new_days.replace(" ", ",")
            #  print(new_days)
            new_days = new_days.split(',')
            my_list = []
            for i in range(len(new_days)):
                y = "'" + new_days[i] + "'"
                my_list.append(y)
            new_days = ','.join(my_list)
            result_list = self.retrieve_meter_kwh_by_week(meter, new_days)
            return result_list
