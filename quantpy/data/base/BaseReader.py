import abc
import requests
import multiprocessing
import itertools
import time
from pebble import ProcessPool
from requests.exceptions import Timeout


class BaseReader(object):

    def __init__(self, symbols, retry_count=3, pause=0.1, timeout=2):
        """
        Initializer method for the BaseReader class.
        :param symbols: The list of symbols to be used.
        :type symbols: str
        :param retry_count: Optional. The amount of times to retry an api call.
        :type retry_count int
        :param pause: Optional. The amount of time to pause between retries.
        :type pause float
        :param timeout: Optional. The amount of time until a request times out.
        :type timeout int
        """

        self.symbols = self._sanitize_symbols(symbols)
        self.retry_count = retry_count
        self.pause = pause
        self.timeout = timeout

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

    def _sanitize_symbols(self, symbols):
        """
        Symbols can be a string or a list. It will turn them into a list.
        :param symbols: A list of the str symbols.
        :return: list
        """

        if isinstance(symbols, str):
            symbols = symbols.split(' ')

        return symbols

    def _sanitize_data(self, data):
        try:
            data = self._organize_data(data)

        except Exception as e:
            data = self._check_data(data)

        return data

    @abc.abstractmethod
    def _check_data(self, data):
        """
        Method to check the data for errors. Must be implemented by a subclass.
        :return: The sanitized data.
        """

        raise NotImplementedError('Subclass has not implemented property.')

    @abc.abstractmethod
    def _organize_data(self, data):
        """
        Method to check the data for errors. Must be implemented by a subclass.
        :return: The sanitized data.
        """

        raise NotImplementedError('Subclass has not implemented property.')

    def read(self):
        """
        Function to read the requested data.
        :return: symbol_dict a dictionary mapping symbols to their data.
        :rtype dict
        """

        if len(self.symbols) > 1:
            # If more than one symbol requested, then do a multiprocess read.
            symbol_dict, times = self.multi_read()
        else:
            # If only one symbol requested, then do a single read.
            symbol_dict, times = self.single_read(self.symbols[0])

        return symbol_dict, times

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

    def single_read(self, symbol, session=None):
        """
        Function to read a single symbol from the requested url and sanitize the
        data from the request.
        :param symbol: The symbol being requested.
        :type symbol str
        :param session: The session to be used when requesting. Default is None.
        :type requests.Session()
        :return: A dictionary mapping the symbol to its data.
        :rtype dict
        """

        cur = time.time()

        try:
            if session:
                # If using a session, then send the request using the session.
                data = session.get(url=self._url.format(symbol),
                                   params=self._params,
                                   timeout=self.timeout)
            else:
                # If not using a session, then use requests to send the request.
                data = requests.get(url=self._url.format(symbol),
                                    params=self._params,
                                    timeout=self.timeout)

                data_dict = {symbol: self._sanitize_data(data.json())}

        except Timeout as to:
            # Catches a Timeout exception if a symbols information took to long.
            data_dict = {symbol: to}

        except Exception as e:
            # Catches all other errors related to getting the information.
            data_dict = {symbol: e}

        return data_dict, {symbol: round(time.time() - cur, 2)}

    def single_read_wrapper(self, arguments):
        return self.single_read(*arguments)
