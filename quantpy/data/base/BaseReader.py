import requests
import abc
import multiprocessing
from concurrent.futures import TimeoutError
from pebble import ProcessPool
import itertools

class BaseReader(object):

    def __init__(self, symbols, start=None, end=None, interval=None,
                 retry_count=3, pause=0.1, timeout=30):
        """
        Initializer method for the BaseReader class.
        :param symbols: The list of symbols to be used.
        :type symbols: str
        :param start: Optional. The start date. Default is 5 years before today.
        :type start str, int, date, datetime, Timestamp
        :param end: Optional. The end date. Default is today's date.
        :type end str, int, date, datetime, Timestamp
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

        self.symbols = symbols
        self.start = start
        self.end = end
        self.retry_count = retry_count
        self.pause = pause
        self.timeout = timeout
        self.pause_multiplier = 1
        self.interval = interval

    @property
    @abc.abstractmethod
    def _default_start_date(self):
        """
        Method to get the default start date. Needs to be implemented in a
        subclass.
        """

        raise NotImplementedError('Subclass has not implemented property.')

    @property
    @abc.abstractmethod
    def _default_end_date(self):
        """
        Method to get the default end date. Needs to be implemented in a
        subclass.
        """

        raise NotImplementedError('Subclass has not implemented property.')

    @property
    @abc.abstractmethod
    def _url(self):
        """
        Method to get the url of the API endpoint. Needs to be implemented in
        a subclass.
        """

        raise NotImplementedError('Subclass has not implemented property.')

    @property
    @abc.abstractmethod
    def _params(self):
        """
        Method to get the parameters for the API endpoint. Needs to be
        implemented in a subclass.
        :return:
        """

        raise NotImplementedError('Subclass has not implemented property.')

    @property
    @abc.abstractmethod
    def _chunksize(self):
        """
        Method to determine the chunksize to process requests. Needs to be
        implemented in a subclass.
        :return:
        """

        raise NotImplementedError('Subclass has not implemented property.')

    @abc.abstractmethod
    def _sanitize_dates(self, start=None, end=None):
        """
        Method to sanitize the start and end date to match the format required
        for an API endpoint. Needs to be implemented in a subclass.
        :param start: Optional. The start date. Default is 5 years before today.
        :type start str, int, date, datetime, Timestamp
        :param end: Optional. The end date. Default is today's date.
        :type end str, int, date, datetime, Timestamp
        :return: start, end tuple of the formatted dates.
        """

        raise NotImplementedError('Subclass has not implemented method.')

    @abc.abstractmethod
    def _sanitize_symbols(self, symbols):
        """
        Method to sanitize the symbols for the API endpoint. Needs to be
        implemented in a subclass.
        :param start: Optional. The start date. Default is 5 years before today.
        :type start str, int, date, datetime, Timestamp
        """

        raise NotImplementedError('Subclass has not implemented method.')

    @staticmethod
    @abc.abstractmethod
    def _sanitize_data(data=None):
        """
        Abstract method to sanitize the data once it is received from the API
        endpoint. Needs to be implemented in a subclass.
        :param data: The json data from the endpoint.
        :return: A formatted dataframe.
        """

        raise NotImplementedError('Subclass has not implemented method.')

    def read(self):

        if len(self.symbols) > 1:
            symbol_json_dict = self.multi_read()
        else:
            symbol_json_dict = self.single_read(self.symbols[0])

        symbol_data_dict = {symbol: self._sanitize_data(json_data)
                            for symbol, json_data in symbol_json_dict.items()}

        return symbol_data_dict

    def single_read(self, symbol, session=None):
        print('Getting {}'.format(symbol))

        if not session:
            data = requests.get(url=self._url.format(symbol),
                                params=self._params,
                                timeout=self.timeout).json()
        else:
            data = session.get(url=self._url.format(symbol),
                               params=self._params,
                               timeout=self.timeout).json()

        return {symbol: data}

    def multi_read(self):
        index = 0
        symbol_json_dict = {}
        session = requests.Session()

        with ProcessPool(multiprocessing.cpu_count()) as pool:
            # Gets a ProcessMapFuture object using the ProcessPool's .map method.
            # The .map method completes the processes asynchronously.
            results = pool.map(self.single_read_wrapper, zip(self.symbols,
                                                             itertools.repeat(session)))

            # Gets an iterator from the ProcessPool's .map method result.
            results_interator = results.result()

            # Iterate through the iterator's results catching all timeout exceptions
            # and stop exceptions.
            while True:
                try:
                    # Gets the next value in the iterator.
                    symbol_data = next(results_interator)

                    # Appends it to the symbol tuple list.
                    symbol_json_dict.update(symbol_data)

                except StopIteration:
                    # Iterators throw a StopIteration exception when they are done.
                    break

                except TimeoutError as te:
                    # Catches a TimeoutError if a symbols information took to long.
                    pass

                except Exception as e:
                    # Catches all other errors related to getting the information.
                    pass

                finally:
                    index += 1

        session.close()

        return symbol_json_dict

    def single_read_wrapper(self, arguments):
        return self.single_read(*arguments)