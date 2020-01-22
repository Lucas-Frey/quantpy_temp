import abc

class BaseReader(object):

    def __init__(self, symbols, start=None, end=None, interval=None,
                 retry_count=3, pause=0.1, timeout=2):
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

    @abc.abstractmethod
    def read(self):
        """
        Method to read the data.
        :return:
        """
        raise NotImplementedError('Subclass has not implemented method.')

    @abc.abstractmethod
    def single_read(self, symbol, session=None):
        """

        :param symbol:
        :param session:
        :return:
        """

        raise NotImplementedError('Subclass has not implemented method.')

    @abc.abstractmethod
    def multi_read(self):
        raise NotImplementedError('Subclass has not implemented method.')
