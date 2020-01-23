import abc
from quantpy.data.base.BaseReader import BaseReader


class BaseQuoteReader(BaseReader):

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
        """

        self.start, self.end = self._sanitize_dates(start, end)
        self.interval = interval

        super().__init__(symbols, retry_count, pause, timeout)

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