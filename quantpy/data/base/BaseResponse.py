import abc


class BaseResponse:

    def __init__(self, symbol, exception=None):

        self._symbol = symbol
        self._exception = exception

    @property
    def symbol(self):
        return self._symbol

    @property
    def exception(self):
        return self._exception

    @abc.abstractmethod
    def _handle_read(self, summary_object):
        raise NotImplementedError('Subclass has not implemented property.')

    @abc.abstractmethod
    def _handle_write(self, value, error):
        raise NotImplementedError('Subclass has not implemented property.')

    class SummaryObject:
        def __init__(self):
            self._included = False
            self._value = None
            self._error_occurred = False
            self._error = None
            self._string_value = ''

        @property
        def value(self):
            return self._value

        @value.setter
        def value(self, value):
            self._included = True
            self._value = value

        @property
        def included(self):
            return self._included

        @property
        def error(self):
            return self._error

        @error.setter
        def error(self, value):
            self._error_occurred = True
            _error = value

        @property
        def error_occurred(self):
            return self._error_occurred
