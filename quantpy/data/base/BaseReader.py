import abc
import requests
import multiprocessing
import itertools
import time
from pebble import ProcessPool
from requests.exceptions import Timeout


class BaseReader(object):

    def __init__(self, symbols, timeout=5):
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

        self._symbols = self._parse_symbols(symbols)
        self._timeout = timeout
        self._read_called = False

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

    @abc.abstractmethod
    def _check_data(self, symbol, data):
        """
        Method to check the data for errors. Must be implemented by a subclass.
        :return: The sanitized data.
        """

        raise NotImplementedError('Subclass has not implemented property.')

    @abc.abstractmethod
    def _check_init_args(self):
        """
        Method to check that the init args are in a proper configuration. Must be implemented by a subclass.
        :return: The sanitized data.
        """

        raise NotImplementedError('Subclass has not implemented property.')

    @abc.abstractmethod
    def _parse_data(self, symbol, data):
        """
        Method to check the data for errors. Must be implemented by a subclass.
        :return: The sanitized data.
        """

        raise NotImplementedError('Subclass has not implemented property.')

    @abc.abstractmethod
    def _handle_read_exception(self, symbol, exception):
        raise NotImplementedError('Subclass has not implemented property.')

    def _parse_symbols(self, symbols):
        """
        Symbols can be a string or a list. It will turn them into a list.
        :param symbols: A list of the str symbols.
        :return: list
        """

        if isinstance(symbols, str):
            symbols = symbols.split(' ')

        return symbols

    def _parse_response(self, symbol, data):
        try:
            data = self._parse_data(symbol, data.json())

        except Exception as e:
            data = self._check_data(symbol, data)

        return data

    def read(self):
        """
        Function to read the requested data.
        :return: symbol_data a dictionary mapping symbols to their data.
        :rtype dict
        """

        self._check_init_args()

        if len(self._symbols) > 1:
            # If more than one symbol requested, then do a multiprocess read.
            symbol_data = self.multi_read()
        else:
            # If only one symbol requested, then do a single read.
            symbol_data = self.single_read(self._symbols[0])

        self._read_called = True

        return symbol_data

    def multi_read(self):
        symbol_json_dict = {}
        session = requests.Session()

        with ProcessPool(multiprocessing.cpu_count()) as pool:
            # Gets a ProcessMapFuture object using the ProcessPool's .map method.
            # The .map method completes the processes asynchronously.
            results = pool.map(self.single_read_wrapper,
                               zip(self._symbols, itertools.repeat(session)))

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

                except StopIteration:
                    # Iterators throw a StopIteration exception when they are done.
                    break

        session.close()

        return symbol_json_dict

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

        try:
            if session:
                # If using a session, then send the request using the session.
                data = session.get(url=self._url.format(symbol),
                                   params=self._params,
                                   timeout=self._timeout)
            else:
                # If not using a session, then use requests to send the request.
                data = requests.get(url=self._url.format(symbol),
                                    params=self._params,
                                    timeout=self._timeout)

            parsed_data = self._parse_response(symbol, data)

            return parsed_data

        except Timeout as to:
            # Catches a Timeout exception if a symbols information took to long.
            exception_data = self._handle_read_exception(symbol, to)
            return exception_data

        except Exception as e:
            # Catches all other errors related to getting the information.
            exception_data = self._handle_read_exception(symbol, e)
            return exception_data

    def single_read_wrapper(self, arguments):
        return self.single_read(*arguments)
