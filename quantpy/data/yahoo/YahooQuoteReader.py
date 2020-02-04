import datetime
import time
import pandas as pd
import quantpy.data.yahoo.YahooExceptions as YahooExceptions
from quantpy.data.base.BaseReader import BaseReader
from quantpy.data.yahoo.YahooQuoteResponse import YahooQuoteResponse
from quantpy.data.yahoo.YahooQuoteResponse import YahooQuoteResponse


class YahooQuoteReader(BaseReader):

    def __init__(self, symbols, start=None, end=None, period='max', interval='1d', events=False, pre_post=True,
                 timeout=2):
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
        :param timeout: The amount of time until a request times out.
        :type timeout int
        """

        # Since symbols, start, and end can be input in a variety of forms, they
        # are formatted to be complacent with the API request.
        self.__start, self.__end = self._sanitize_dates(start, end, period)
        self.__interval = interval
        self.__use_period_flag = False
        self.__events = events
        self.__pre_post = pre_post

        # Call the super class' constructor.
        super().__init__(symbols, timeout)

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
        :return: A dictionary of parameters. This is called after sanitization.
        :rtype dict
        """
        if self.__events:
            events_param = 'div,splits'
        else:
            events_param = ''

        return {'period1': self.__start, 'period2': self.__end,
                'interval': self.__interval, 'includePrePost': self.__pre_post,
                'events': events_param}

    def _check_init_args(self):
        pass

    def _parse_response_error(self, symbol, error):
        pass

    def _parse_response(self, symbol, response_data_json=None):
        response_data = response_data_json.json()

        try:
            quotes_dict = response_data['chart']['result'][0]
        except Exception:
            return self._parse_quote_error(symbol, response_data_json)

        yq = YahooQuoteResponse(symbol)

        yq.quote = self._parse_quote(quotes_dict)

        yq.meta = self._parse_quote_meta(quotes_dict)

        if self.__events:
            yq.dividends = self._parse_quote_dividends(quotes_dict)
            yq.splits = self._parse_quote_splits(quotes_dict)

        return yq

    def _parse_quote_error(self, symbol, response_data_json):
        pass

    def _parse_quote(self, quotes_dict):
        try:
            quotes = quotes_dict['indicators']['quote'][0]
            dates = quotes_dict['timestamp']
            adj_close = quotes_dict['indicators']['adjclose'][0]

        except Exception as e:
            return None, YahooExceptions.YahooIndicatorNotFoundError(e)

        try:
            # Combine the two dictionaries.
            quote_dictionary = {**quotes, **adj_close}

            # Turn the dictionary into a dataframe.
            quote_dataframe = pd.DataFrame.from_dict(quote_dictionary)

            # Get the timestamp (datetime) for each quote entry in the data and parse it.
            quote_dataframe['date'] = dates

            quote_dataframe.reindex(columns=['date', 'open', 'high', 'low', 'close', 'adjclose', 'volume'])

        except Exception as e:
            return None, YahooExceptions.YahooIndicatorFormatError(e)

        return quote_dataframe, None

    def _parse_quote_meta(self, quotes_dict):
        return None, None

    def _parse_quote_dividends(self, quotes_dict):
        try:
            # Get the splits dictionary from the JSON API output.
            dividends_dict = quotes_dict['chart']['result'][0]['events']['dividends']
        except Exception as e:
            return None, YahooExceptions.YahooIndicatorNotFoundError(e)

        try:
            # Convert the splits dictionary to a Dataframe.
            dividends_dataframe = pd.DataFrame.from_dict(dividends_dict, orient='index')
            dividends_dataframe.index = pd.Index(range(0, dividends_dataframe.shape[0]))
        except Exception as e:
            return None, YahooExceptions.YahooIndicatorFormatError(e)

        return dividends_dataframe, None

    def _parse_quote_splits(self, quotes_dict):
        try:
            # Get the splits dictionary from the JSON API output.
            splits_dict = quotes_dict['chart']['result'][0]['events']['splits']
        except Exception as e:
            return None, YahooExceptions.YahooIndicatorNotFoundError(e)

        try:
            # Convert the splits dictionary to a Dataframe.
            splits_dataframe = pd.DataFrame.from_dict(splits_dict, orient='index')
            splits_dataframe.index = pd.Index(range(0, splits_dataframe.shape[0]))
        except Exception as e:
            return None, YahooExceptions.YahooIndicatorFormatError(e)

        return splits_dataframe, None

    def _find_end_date(self, start, period):
        if period is not None:
            if period == '1d':
                return start + 86400000
            elif period == '5d':
                return start + 432000000
            elif period == '1mo':
                return start + 2592000000
            elif period == '6mo':
                return start + 15552000000
            elif period == '1y':
                return start + 31536000000
            elif period == '2y':
                return start + 63072000000
            elif period == '5y':
                return start + 157680000000
            elif period == '10y':
                return start + 315360000000
            elif period == 'ytd':
                return start + 31536000000
            elif period == 'max':
                return self._default_end_date
            else:
                raise ValueError('Period not properly defined.')

    def _find_start_date(self, end, period):
        if period is not None:
            if period == '1d':
                return end - 86400000
            elif period == '5d':
                return end - 432000000
            elif period == '1mo':
                return end - 2592000000
            elif period == '6mo':
                return end - 15552000000
            elif period == '1y':
                return end - 31536000000
            elif period == '2y':
                return end - 63072000000
            elif period == '5y':
                return end - 157680000000
            elif period == '10y':
                return end - 315360000000
            elif period == 'ytd':
                return end - 31536000000
            elif period == 'max':
                return self._default_start_date
            else:
                raise ValueError('Period not properly defined.')

    def _sanitize_dates(self, start=None, end=None, period=None):
        """
        Function to parse the dates into unix format.
        :param start: The start date of the historical data range.
        :param end: The end date of the historical data range.
        :return: A tuple containing the formatted start and end date.
        :rtype tuple
        """

        if start is not None:
            if isinstance(start, datetime.datetime):
                # If start date is a datetime object, then convert it to unix.
                start = start.timestamp()
            else:
                # Attempt to convert the string to an int.
                start = int(time.mktime(time.strptime(str(start), '%Y-%m-%d')))

        if end is not None:
            if isinstance(end, datetime.datetime):
                # If end date is a datetime object, then convert it to unix.
                end = end.timestamp()
            else:
                # Attempt to convert the string to an int.
                end = int(time.mktime(time.strptime(str(end), '%Y-%m-%d')))

        if start is not None and end is not None and period is None:
            return start, end

        elif start is not None and end is None and period is not None:
            end = self._find_end_date(start, period)

            return start, end

        elif start is None and end is not None and period is not None:
            start = self._find_start_date(start, period)

            return start, end

        elif start is None and end is None and period is not None:
            end = int(time.mktime(datetime.datetime.now().timetuple()))

            start = self._find_start_date(end, period)

            return start, end,
        else:
            raise ValueError('Start, End, and Period incorrectly defined.')
