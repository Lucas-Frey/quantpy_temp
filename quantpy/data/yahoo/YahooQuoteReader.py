import datetime
import time
import pandas as pd
import requests
import multiprocessing
from requests.exceptions import Timeout
from pebble import ProcessPool
import itertools

from quantpy.data.base.BaseReader import BaseReader


class YahooQuoteReader(BaseReader):

    def __init__(self, symbols, start=None, end=None, interval='1d',
                 retry_count=3, pause=0.1, timeout=2, session=None):
        """
        Initializer method for the YahooQuoteReader class.
        :param symbols: The list of symbols to be used.
        :type symbols: str
        :param start: Optional. The start date. Default is 5 years before today.
        :type start str, int, date, datetime, Timestamp
        :param end: Optional. The end date. Default is today's date.
        :type end str, int, date, datetime, Timestamp
        :param date_range: Optional. The range of dates. Either used start and end or date_range.
        :type str
        :param interval: Optional. The interval to be used.
        :type interval str
        :param retry_count: Optional. The amount of times to retry an api call.
        :type retry_count int
        :param pause: Optional. The amount of time to pause between retries.
        :type pause float
        :param timeout: Optional. The amount of time until a request times out.
        :type timeout int
        :param session: Optional. The requests.session.Session to be used.
        :type requests.session.Session
        """

        symbols = self._sanitize_symbols(symbols)
        start, end = self._sanitize_dates(start, end)

        super(YahooQuoteReader, self).__init__(symbols=symbols,  start=start,
                                               end=end, interval=interval,
                                               retry_count=retry_count,
                                               pause=pause, timeout=timeout)

    @property
    def _default_start_date(self):
        """
        Method to get the default unix start date (January 1st, 1900). Note,
        this date will be negative.
        """
        return int(time.mktime(datetime.datetime(1970, 1, 1).timetuple()))

    @property
    def _default_end_date(self):
        """
        Method to get the default unix end date (Today).
        """

        return int(time.mktime(datetime.datetime.now().timetuple()))

    @property
    def _url(self):
        """
        Method to get the url of the API endpoint for Yahoo! Finance.
        """

        return 'https://query1.finance.yahoo.com/v8/finance/chart/{}'

    @property
    def _params(self):
        """
        Method to get the parameters for the API endpoint.
        :return: A dictionary of the parameters.
        :rtype dict
        """

        return {'period1': self.start, 'period2': self.end,
                'interval': self.interval, 'includePrePost': True, 'events': ''}

    def _sanitize_symbols(self, symbols):

        if isinstance(symbols, str):
            symbols = symbols.split(' ')

        return symbols

    def _sanitize_dates(self, start=None, end=None):
        """
        Function to parse the dates into milliseconds since epoch format and
        return the correct params argument.
        :param start: The start date of the historical data range.
        :param end: The end date of the historical data range.
        :return: returns a dictionary of the required parameters.
        """

        if not start:
            # If no start date specified, then set it to the default value.
            start = self._default_start_date
        elif isinstance(start, datetime.datetime):
            # If start date is a datetime object, then convert it to unix.
            start = start.timestamp()
        else:
            # Attempt to convert to a string.
            start = int(time.mktime(time.strptime(str(start), '%Y-%m-%d')))

        if not end:
            # If no end date specified, then set it to the default value.
            end = self._default_end_date
        elif isinstance(end, datetime.datetime):
            # If end date is a datetime object, then convert it to unix.
            end = end.timestamp()
        else:
            # Attempt to convert to a string.
            end = int(time.mktime(time.strptime(str(end), '%Y-%m-%d')))

        return start, end

    def _check_errors(self, data=None):
        # If Yahoo is unresponsive, then throws RuntimeError.
        if 'Will be right back' in data.text:
            raise RuntimeError('Yahoo Finance is currently down.')

        # Convert data to a JSON object.
        data_json = data.json()

        # If Yahoo sends an error, then through a RuntimeError.
        if ('chart' in data_json) and (data_json['chart']['error']):
            raise RuntimeError('Error received from Yahoo: {}'.format(
                str(data_json['chart']['error']['description'])))

        # If Yahoo sends no data, then through a RuntimeError.
        if ('chart' not in data_json) or (data_json['chart']['result'] is None) or \
                (not data_json['chart']['result']):
            raise RuntimeError('No data recieved from Yahoo.')

    def read(self):

        if len(self.symbols) > 1:
            symbol_dict, times = self.multi_read()
        else:
            symbol_dict, times = self.single_read(self.symbols[0])

        return symbol_dict, times

    def single_read(self, symbol, session=None):
        cur = time.time()

        try:
            if session:
                data = session.get(url=self._url.format(symbol),
                                   params=self._params,
                                   timeout=1)
            else:
                data = requests.get(url=self._url.format(symbol),
                                    params=self._params,
                                    timeout=1)

            data_dict = {symbol: self._sanitize_data(data.json())}

        except Timeout as to:
            # Catches a TimeoutError if a symbols information took to long.
            data_dict = {symbol: to}

        except TypeError as te:
            # If Yahoo is unresponsive, then throws RuntimeError.
            if 'Will be right back' in data.text:
                data_dict = {symbol: 'Yahoo Finance is currently down.'}
            else:
                data = data.json()

                # If Yahoo sends an error, then through a RuntimeError.
                if ('chart' in data) and (data['chart']['error']):
                    data_dict = {symbol: str(data['chart']['error']['description'])}

                # If Yahoo sends no data, then through a RuntimeError.
                elif ('chart' not in data) or (data['chart']['result'] is None) or (not data['chart']['result']):
                    data_dict = {symbol: 'No data recieved from Yahoo.'}

                else:
                    data_dict = {symbol: 'No data'}

            print(data_dict[symbol])

        except Exception as e:
            print(e)
            # Catches all other errors related to getting the information.
            data_dict = {symbol: e}

        return data_dict, {symbol: round(time.time() - cur, 2)}

    def multi_read(self):
        symbol_json_dict = {}
        times = {}
        session = requests.Session()

        with ProcessPool(multiprocessing.cpu_count()) as pool:
            # Gets a ProcessMapFuture object using the ProcessPool's .map method.
            # The .map method completes the processes asynchronously.
            results = pool.map(self.single_read_wrapper,
                               zip(self.symbols, itertools.repeat(session)))

            # Gets an iterator from the ProcessPool's .map method result.
            results_interator = results.result()

            # Iterate through the iterator's results catching all timeout exceptions
            # and stop exceptions.
            while True:
                try:
                    # Gets the next value in the iterator.
                    symbol_data = next(results_interator)

                    # Appends it to the symbol tuple list.
                    symbol_json_dict.update(symbol_data[0])
                    times.update(symbol_data[1])
                except StopIteration:
                    # Iterators throw a StopIteration exception when they are done.
                    break

        session.close()

        return symbol_json_dict, times

    def single_read_wrapper(self, arguments):
        return self.single_read(*arguments)

    @staticmethod
    def _sanitize_data(data=None):
        # Get the open, high, low, close, volume (OHLCV) data from the JSON.

        ohlcv_data = data['chart']['result'][0]['indicators']['quote'][0]

        # Get the adjusted close (adjclose) data from the JSON.
        adj_close_data = data['chart']['result'][0]['indicators']['adjclose'][0]

        # Combine the two dictionaries.
        quote_dictionary = {**ohlcv_data, **adj_close_data}

        # Turn the dictionary into a dataframe.
        quote_dataframe = pd.DataFrame.from_dict(quote_dictionary)

        # Get the timestamp (datetime) for each quote entry in the data and parse it.
        quote_dataframe['date'] = data['chart']['result'][0]["timestamp"]

        quote_dataframe['date'] = pd.to_datetime(quote_dataframe['date'], unit='s')
        quote_dataframe['date'] = quote_dataframe['date'].dt.tz_localize('UTC')

        quote_dataframe.index = pd.Index(range(0, quote_dataframe.shape[0]))

        quote_dataframe = quote_dataframe.reindex(columns=['date', 'open', 'high',
                                                           'low', 'close',
                                                           'adjclose', 'volume'])

        return quote_dataframe
