import datetime
import time
import pandas as pd
from quantpy.data.base.BaseQuoteReader import BaseQuoteReader
import quantpy.data.yahoo.YahooExceptions as YahooExceptions


class YahooQuoteReader(BaseQuoteReader):

    def __init__(self, symbols, start=None, end=None, interval='1d', events=False,
                 pre_post=True, retry_count=3, pause=0.1, timeout=2):
        """
        Initializer method for the YahooQuoteReader class.
        :param symbols: The list of symbols to be used.
        :type symbols: str or list
        :param start: Optional. The start date. Default is 5 years before today.
        :type start str, int, date, datetime, Timestamp
        :param end: Optional. The end date. Default is today's date.
        :type end str, int, date, datetime, Timestamp
        :param interval: Optional. The interval to be used.
        :type interval str
        :param events: Include splits and dividends?
        :type events bool
        :param retry_count: Optional. The amount of times to retry an api call.
        :type retry_count int
        :param pause: Optional. The amount of time to pause between retries.
        :type pause float
        :param timeout: Optional. The amount of time until a request times out.
        :type timeout int
        """

        # Since symbols, start, and end can be input in a variety of forms, they
        # are formatted to be complacent with the API request.
        symbols = self._sanitize_symbols(symbols)
        start, end = self._sanitize_dates(start, end)

        self.events = events
        self.pre_post = pre_post

        # Call the super class' constructor.
        super().__init__(symbols, start, end, interval, retry_count, pause,
                         timeout)

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
        :return: A dictionary of parameters. This is called after sanitization.
        :rtype dict
        """
        if self.events:
            events_param = 'div,splits'
        else:
            events_param = ''

        return {'period1': self.start, 'period2': self.end,
                'interval': self.interval, 'includePrePost': self.pre_post,
                'events': events_param}

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

    def _sanitize_dates(self, start=None, end=None):
        """
        Function to parse the dates into unix format.
        :param start: The start date of the historical data range.
        :param end: The end date of the historical data range.
        :return: A tuple containing the formatted start and end date.
        :rtype tuple
        """

        if not isinstance(start, int):
            if not start:
                # If no start date specified, then set it to the default value.
                start = self._default_start_date
            elif isinstance(start, datetime.datetime):
                # If start date is a datetime object, then convert it to unix.
                start = start.timestamp()
            else:
                # Attempt to convert the string to an int.
                start = int(time.mktime(time.strptime(str(start), '%Y-%m-%d')))

        if not isinstance(end, int):
            if not end:
                # If no end date specified, then set it to the default value.
                end = self._default_end_date
            elif isinstance(end, datetime.datetime):
                # If end date is a datetime object, then convert it to unix.
                end = end.timestamp()
            else:
                # Attempt to convert the string to an int.
                end = int(time.mktime(time.strptime(str(end), '%Y-%m-%d')))

        return start, end

    def _organize_data(self, data=None):
        quote_data = self._organize_quote_data(data)

        if self.events:
            dividends_data = self._organize_events_data(data, 'dividends')
            splits_data = self._organize_events_data(data, 'splits')

        else:
            dividends_data = None
            splits_data = None

        return quote_data, dividends_data, splits_data

    def _organize_quote_data(self, data):
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

        return quote_dataframe.reindex(columns=['date', 'open', 'high',
                                                'low', 'close',
                                                'adjclose', 'volume'])

    def _organize_events_data(self, data, event_type=None):
        # Get the splits dictionary from the JSON API output.
        event_dict = data['chart']['result'][0]['events'][event_type]

        # Convert the splits dictionary to a Dataframe.
        event_dataframe = pd.DataFrame.from_dict(event_dict, orient='index')

        event_dataframe['date'] = pd.to_datetime(event_dataframe['date'], unit='s')
        event_dataframe['date'] = event_dataframe['date'].dt.tz_localize('UTC')

        event_dataframe.index = pd.Index(range(0, event_dataframe.shape[0]))

        return event_dataframe

    def _check_data(self, data=None):

        if 'Will be right back' in data.text:
            error = YahooExceptions.YahooRuntimeError('Yahoo Finance is currently down.')

        elif data.json()['chart']['error']:
            error = YahooExceptions.YahooRequestError(str(data.json()['chart']['error']['description']))

        elif not data.json()['chart'] or not data['chart']['result']:
            error = YahooExceptions.YahooRequestError('No data')

        else:
            error = YahooExceptions.YahooError('An error occurred in Yahoo\'s response.')

        return error