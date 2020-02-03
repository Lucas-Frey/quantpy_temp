from quantpy.data.base.BaseResponse import BaseResponse
import warnings


class YahooQuoteResponse(BaseResponse):

    def __init__(self, symbol, exception):
        self.__quote = None

        super().__init__(symbol=symbol, exception=exception)

    def _handle_read(self, summary_object):
        if self._exception is None:
            if summary_object:
                if summary_object.included:
                    return summary_object.value
                elif summary_object.error_occurred:
                    raise Exception(summary_object.error)
                else:
                    return None
            else:
                warnings.warn('The value referenced and was never assigned.')
                return None
        else:
            warnings.warn(str(self._exception))
            return None

    def _handle_write(self, value, error):
        summary_object = self.SummaryObject()

        if value is not None and error is not None:
            raise ValueError('Cannot assign both a value and an error.')
        else:
            if value is not None:
                summary_object.value = value
            else:
                summary_object.error = error

        return summary_object

    @property
    def quote(self):
        return self._handle_read(self.__quote)

    @quote.setter
    def quote(self, value, error=None):
        self._handle_write(value, error)
