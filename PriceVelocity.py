# PriceVelocity was developed by Alejandro Robles and David L Greenberg

import os
import datetime
import numpy
import sqlalchemy as sa
import pandas as pd
import traceback

# create a parent class for all types of fuel
class Fuel(object):
    dburl = os.environ.get('SOME_ENV_VAR')
    engine = sa.create_engine(dburl)
    restricted = False
    grade = 'regular'
    age = {'regular': 'reg_price_age', 'midgrade': 'mid_price_age', 'premium': 'pre_price_age',
           'diesel': 'des_price_age'}

    def __init__(self, id, miles, numdays, retail=None, refresh_retail_data=True):
        self.id = id
        self.miles = miles
        self.numdays = numdays
        self.DIST = self.get_distance_matrix
        if refresh_retail_data:
            self.retail = self.get_retail_data()
        if retail != 'Not Fuel':
            self.retail = self.clean_retail(numdays=numdays)
            self.prices = self.get_prices(id, miles)
            # self.price_velocity = self.price_velocity()

    # Queries a distance matrix data of stations. Should contain a origin location, destination location,
    # and the distance between them.
    @property
    def get_distance_matrix(self):
        query = 'select origin_id, destination_id, distance from distance_matrix ' \
                'where origin_id = ' + str(self.id) + \
                ' and distance < ' + str(self.miles) + ';'
        return pd.read_sql(query, self.engine)

    # Queries from a table called retail that has a collection of observed prices for each station
    # Always want args to be either empty or to have at least 'location_id' and 'last_update'
    def get_retail_data(self, *args):
        # the code below will obviously only work if you have a database full of gas station price data
        # for the purposes of testing this out, just consume the csv data included in the repo
        # you might use something like:
        # return pd.read_csv('/path/to/sample_data.csv'
        try:
            variables = '*'
            if args: variables = self.list_to_string(args)
            stations = self.DIST.destination_id.tolist()
            if len(stations) > 1:
                stations = self.list_to_string(self.DIST.destination_id.tolist()) + ', ' + str(self.id)
                query = 'select ' + variables + ' from retail where location_id in (' + stations + ');'
            elif len(stations) == 0:
                query = 'select ' + variables + ' from retail where location_id = {0}'.format(self.id)
            return pd.read_sql(query, self.engine)
        except Exception as e:
            # if there are any issues with gcloud then return an error
            return 'Error: Damn! Had trouble retrieving data from your query. Please check query\n ' + traceback.print_exc()
            pass

    def clean_retail(self, numdays=5, *args):
        r = self.retail.rename(index=str, columns={"location_id": "station_id", "last_update": "date"})
        r = r[(r['station_id'].notnull()) & (r[args[1]] <= 24.1) & (r[args[0]] != 0)] if args else r[
            r['station_id'].notnull()]
        r['station_id'] = [int(id) for id in r['station_id']]
        r["date"] = pd.to_datetime(r.date)
        r.date = [d.date() for d in r.date]
        return r.drop_duplicates()

    def get_prices(self, id, miles, *args):
        p = pd.merge(self.retail, self.DIST[(self.DIST['origin_id'] == id) & (self.DIST.distance < miles)],
                     left_on='station_id',
                     right_on='destination_id')
        if args: p = p[list(args)]
        p = pd.concat([Fuel.get(id, df=self.retail), p]).drop(self.age[self.grade], axis=1).fillna(0)
        p['station_id'] = p['station_id'].astype('category')
        #  self.data = self.data.drop_duplicates(subset='price_date', keep='last')
        return p.drop_duplicates(subset=('date', 'station_id'), keep='last')

    # a little method to provide some statistical data for a specific station
    def compare(self, to_sheet=False, print_output=False):
        df = self.prices.describe()
        d = datetime.datetime.today()
        d_str = str(d.date().year) + str(d.date().month) + str(d.date().day) + '_' + str(d.hour) + str(d.minute)
        analysis_id = d_str + '_' + str(self.id)
        cluster = df[self.grade]
        station = Fuel.get(self.id, df=self.prices)[self.grade]
        headers = ['date', 'station', 'fuel_type', 'station_mean', 'cluster_mean', 'station_min', 'cluster_min',
                   'station_max', 'cluster_max', 'analysis_id']
        data = [datetime.datetime.today().date(), self.id, self.grade, station.mean(), cluster['mean'],
                station.min(), cluster['min'], station.max(), cluster['max'], analysis_id]
        df = pd.DataFrame(data=[data], columns=headers)
        if print_output == True:
            print(
            'Statistical summary for ' + str(len(numpy.unique(self.prices.station_id))) + ' stations that are ' + str(
                self.miles) + ' mile radius of station '
            + str(self.id) + ' for the span of ' + str(self.numdays) + ' days.')
            print('You are above the mean by ' + str(station.mean() - cluster['mean']) + '\n') if station.mean() > \
                                                                                                  cluster[
                                                                                                      'mean'] else 'You are below the mean by ' + str(
                station.mean() - cluster['mean'])
        return df

    def compare_by_date(self):
        datelist = Fuel.get(self.id, df=self.prices).date.unique().tolist()
        for date in datelist:
            p = Fuel.get_by_date(str(date), df=self.prices)
            self.compare(p)

    @staticmethod
    def list_to_string(arr):
        return str(arr).strip('[').strip(']')

    @staticmethod
    def format_date(d):
        index = d.find("/", 3)
        return d[:index + 1] + d[index + 1:]

    @staticmethod
    def get_datetime(date, format="%Y-%m-%d %H:%M:%S", reformat=False):
        if reformat: date = Fuel.format_date(date)
        return datetime.datetime.strptime(date, format).date()

    @staticmethod
    def get(value, df, variable='station_id'):
        return df[df[variable] == value]

    @staticmethod
    def get_by_date(date, df, format='%Y-%m-%d', variable='date'):
        return df[df[variable] == Fuel.get_datetime(date, format)]

    @staticmethod
    def ucount(arr):
        return len(numpy.unique(arr))


# child classes for each grade of fuel
class Regular(Fuel):
    restricted = True
    grade = 'regular'

    def __init__(self, id, miles, numdays, retail='Not Fuel'):
        super(Regular, self).__init__(id, miles, numdays, retail)
        self.retail = self.clean_retail(numdays, self.grade, self.age[self.grade])
        self.prices = self.get_prices(id, miles, 'station_id', self.grade, 'date', 'distance')


class Midgrade(Fuel):
    restricted = True
    grade = 'midgrade'

    def __init__(self, id, miles, numdays, retail='Not Fuel'):
        super(Midgrade, self).__init__(id, miles, numdays, retail)
        self.retail = self.clean_retail(numdays, self.grade, self.age[self.grade])
        self.prices = self.get_prices(id, miles, 'station_id', self.grade, 'date', 'distance')


class Premium(Fuel):
    restricted = True
    grade = 'premium'

    def __init__(self, id, miles, numdays, retail='Not Fuel'):
        super(Premium, self).__init__(id, miles, numdays, retail)
        self.retail = self.clean_retail(numdays, self.grade, self.age[self.grade])
        self.prices = self.get_prices(id, miles, 'station_id', self.grade, 'date', 'distance')


class Diesel(Fuel):
    restricted = True
    grade = 'diesel'

    def __init__(self, id, miles, numdays, retail='Not Fuel'):
        super(Diesel, self).__init__(id, miles, numdays, retail)
        self.retail = self.clean_retail(numdays, self.grade, self.age[self.grade])
        self.prices = self.get_prices(id, miles, 'station_id', self.grade, 'date', 'distance')


# *************************** END OF CLASS DEFINITION *******************************************
# SAMPLE USAGE
# f = Fuel(1022,5,15)
# r = Regular(1022,5,15)
# m = Midgrade(1022,5,15)
# p = Premium(1022,5,15)
# d = Diesel(1022,5,15)

# r.compare() this will compare target_id prices with the rest and upload to google sheet
# r.price_velocity() this will rank the market drivers

# ************************** Above is sample code to run to test out the classes *******************

# price_velocity accepts the following parameters:
# prices - a pandas dataframe of all of the prices for the stations in the cluster
# station - the target station (i.e. the one that is used to identify the cluster
# period - the number of days used for price change comparison
# iter - the number of iterations to run for comparison purposes (i.e. how many times to run a comparison over
# a period shifted back by a day
# grade - which fuel grade to use for comparison purposes
# last_day - function defaults to the maximum date contained within the dataframe.

def price_velocity(fuel_obj, station, grade='regular', period=30, iter=1, last_day=None):
    prices = fuel_obj.prices
    grade = fuel_obj.grade
    df_pv = pd.DataFrame(columns=['rank', 'station_id', 'day_lag', 'price_change', 'start_date', 'end_date'])
    ranker = []
    print ('stations: {0}'.format(numpy.unique(prices.station_id).tolist()))

    for it in range(0, iter):
        l = {}
        new_dict = {}
        price_differences = {}
        data = []
        for id in numpy.unique(prices.station_id).tolist():
            # df = Fuel.get(id, df=self.prices.sort_values(['date'], axis=0))
            df = prices[prices.station_id == id].drop_duplicates()
            if last_day == None:
                end_date = df.date.max() - datetime.timedelta(days=it)
            else:
                d = datetime.datetime.strptime(last_day, '%Y-%m-%d')
                end_date = d.date() - datetime.timedelta(days=it)
            start_date = end_date - datetime.timedelta(days=period)
            df = df[(df.date >= start_date) & (df.date <= end_date)]
            initial_price = df[grade].iloc[0]
            for d in df.get('date'):
                p = Fuel.get(d, df, 'date')[grade].get_values()[0]
                if initial_price != p:
                    l[id] = d  # dict
                    price_differences[id] = p - initial_price
                    break
        best_date = min(l.itervalues())
        for k, v in l.iteritems():
            new_dict.setdefault(v, []).append(k)
        for i, w in enumerate(sorted(new_dict)):
            for element in new_dict[w]:
                days = ((w - best_date).total_seconds()) / 86400
                data.append([i + 1, element, days, float(price_differences[element])])
                # print i + 1, w, new_dict[w], w - best_date
        df = pd.DataFrame(data, columns=['rank', 'station_id', 'day_lag', 'price_change'])
        df['start_date'] = str(start_date)
        df['end_date'] = str(end_date)
        df['iter'] = it
        df_pv = df_pv.append(df)
        ranker.append(df['rank'][df['station_id'] == station].values[0])
        # print(i)
    print(str(station) + ':')
    print(ranker)
    return df_pv, ranker


def run():
    r = Regular(1101, 3, 30)
    df, ranker = price_velocity(r, 1101, iter=30, last_day='2017-05-01')
    return df, ranker
