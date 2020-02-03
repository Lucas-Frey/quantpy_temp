from quantpy.data.base.BaseResponse import BaseResponse
import warnings


class YahooSummaryResponse(BaseResponse):

    def __init__(self, symbol, exception=None):
        self.__profile = None
        self.__company_officers = None
        self.__income_statement_history = None
        self.__income_statement_history_quarterly = None
        self.__balance_sheet_history = None
        self.__balance_sheet_history_quarterly = None
        self.__cash_flow_statement_history = None
        self.__cash_flow_statement_history_quarterly = None
        self.__earnings_estimates = None
        self.__earnings_estimates_quarterly = None
        self.__financials_quarterly = None
        self.__financials_yearly = None
        self.__earnings_history = None
        self.__financial_data = None
        self.__default_key_statistics = None

        self.__institution_ownership = None
        self.__insider_holders = None
        self.__insider_transactions = None
        self.__fund_ownership = None
        self.__major_direct_holders = None
        self.__major_direct_holders_breakdown = None

        self.__recommendation_trend = None
        self.__earnings_trend = None
        self.__industry_trend = None
        self.__index_trend_info = None
        self.__index_trend_estimate = None
        self.__sector_trend = None

        self.__calendar_events_earnings = None
        self.__calendar_events_dividends = None
        self.__sec_filings = None
        self.__upgrade_downgrade_history = None
        self.__net_share_purchase_activity = None

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
    def profile(self):
        return self._handle_read(self.__profile)

    @profile.setter
    def profile(self, value=None, error=None):
        self.__profile = self._handle_write(value, error)

    @property
    def company_officers(self):
        return self._handle_read(self.__company_officers)

    @company_officers.setter
    def company_officers(self, value, error=None):
        self.__company_officers = self._handle_write(value, error)

    @property
    def income_statement_history(self):
        return self._handle_read(self.__income_statement_history)

    @income_statement_history.setter
    def income_statement_history(self, value, error=None):
        self.__income_statement_history = self._handle_write(value, error)

    @property
    def income_statement_history_quarterly(self):
        return self._handle_read(self.__income_statement_history_quarterly)

    @income_statement_history_quarterly.setter
    def income_statement_history_quarterly(self, value, error=None):
        self.__income_statement_history_quarterly = self._handle_write(value, error)

    @property
    def balance_sheet_history(self):
        return self._handle_read(self.__balance_sheet_history)

    @balance_sheet_history.setter
    def balance_sheet_history(self, value, error=None):
        self.__balance_sheet_history = self._handle_write(value, error)

    @property
    def balance_sheet_history_quarterly(self):
        return self._handle_read(self.__balance_sheet_history_quarterly)

    @balance_sheet_history_quarterly.setter
    def balance_sheet_history_quarterly(self, value, error=None):
        self.__balance_sheet_history_quarterly = self._handle_write(value, error)

    @property
    def cash_flow_statement_history(self):
        return self._handle_read(self.__cash_flow_statement_history)

    @cash_flow_statement_history.setter
    def cash_flow_statement_history(self, value, error=None):
        self.__cash_flow_statement_history = self._handle_write(value, error)

    @property
    def cash_flow_statement_history_quarterly(self):
        return self._handle_read(self.__cash_flow_statement_history_quarterly)

    @cash_flow_statement_history_quarterly.setter
    def cash_flow_statement_history_quarterly(self, value, error=None):
        self.__cash_flow_statement_history_quarterly = self._handle_write(value, error)

    @property
    def earnings_estimates(self):
        return self._handle_read(self.__earnings_estimates)

    @earnings_estimates.setter
    def earnings_estimates(self, value, error=None):
        self.__earnings_estimates = self._handle_write(value, error)

    @property
    def earnings_estimates_quarterly(self):
        return self._handle_read(self.__earnings_estimates_quarterly)

    @earnings_estimates_quarterly.setter
    def earnings_estimates_quarterly(self, value, error=None):
        self.__earnings_estimates_quarterly = self._handle_write(value, error)

    @property
    def financials_quarterly(self):
        return self._handle_read(self.__financials_quarterly)

    @financials_quarterly.setter
    def financials_quarterly(self, value, error=None):
        self.__financials_quarterly = self._handle_write(value, error)

    @property
    def financials_yearly(self):
        return self._handle_read(self.__financials_yearly)

    @financials_yearly.setter
    def financials_yearly(self, value, error=None):
        self.__financials_yearly = self._handle_write(value, error)

    @property
    def earnings_history(self):
        return self._handle_read(self.__earnings_history)

    @earnings_history.setter
    def earnings_history(self, value, error=None):
        self.__earnings_history = self._handle_write(value, error)

    @property
    def financial_data(self):
        return self._handle_read(self.__financial_data)

    @financial_data.setter
    def financial_data(self, value, error=None):
        self.__financial_data = self._handle_write(value, error)

    @property
    def default_key_statistics(self):
        return self._handle_read(self.__default_key_statistics)

    @default_key_statistics.setter
    def default_key_statistics(self, value, error=None):
        self.__default_key_statistics = self._handle_write(value, error)

    @property
    def institution_ownership(self):
        return self._handle_read(self.__institution_ownership)

    @institution_ownership.setter
    def institution_ownership(self, value, error=None):
        self.__institution_ownership = self._handle_write(value, error)

    @property
    def insider_holders(self):
        return self._handle_read(self.__insider_holders)

    @insider_holders.setter
    def insider_holders(self, value, error=None):
        self.__insider_holders = self._handle_write(value, error)

    @property
    def insider_transactions(self):
        return self._handle_read(self.__insider_transactions)

    @insider_transactions.setter
    def insider_transactions(self, value, error=None):
        self.__insider_transactions = self._handle_write(value, error)

    @property
    def fund_ownership(self):
        return self._handle_read(self.__fund_ownership)

    @fund_ownership.setter
    def fund_ownership(self, value, error=None):
        self.__fund_ownership = self._handle_write(value, error)

    @property
    def major_direct_holders(self):
        return self._handle_read(self.__major_direct_holders)

    @major_direct_holders.setter
    def major_direct_holders(self, value, error=None):
        self.__major_direct_holders = self._handle_write(value, error)

    @property
    def major_direct_holders_breakdown(self):
        return self._handle_read(self.__major_direct_holders_breakdown)

    @major_direct_holders_breakdown.setter
    def major_direct_holders_breakdown(self, value, error=None):
        self.__major_direct_holders_breakdown = self._handle_write(value, error)

    @property
    def recommendation_trend(self):
        return self._handle_read(self.__recommendation_trend)

    @recommendation_trend.setter
    def recommendation_trend(self, value, error=None):
        self.__recommendation_trend = self._handle_write(value, error)

    @property
    def earnings_trend(self):
        return self._handle_read(self.__earnings_trend)

    @earnings_trend.setter
    def earnings_trend(self, value, error=None):
        self.__earnings_trend = self._handle_write(value, error)

    @property
    def industry_trend(self):
        return self._handle_read(self.__industry_trend)

    @industry_trend.setter
    def industry_trend(self, value, error=None):
        self.__industry_trend = self._handle_write(value, error)

    @property
    def index_trend_info(self):
        return self._handle_read(self.__index_trend_info)

    @index_trend_info.setter
    def index_trend_info(self, value, error=None):
        self.__index_trend_info = self._handle_write(value, error)

    @property
    def index_trend_estimate(self):
        return self._handle_read(self.__index_trend_estimate)

    @index_trend_estimate.setter
    def index_trend_estimate(self, value, error=None):
        self.__index_trend_estimate = self._handle_write(value, error)

    @property
    def sector_trend(self):
        return self._handle_read(self.__sector_trend)

    @sector_trend.setter
    def sector_trend(self, value, error=None):
        self.__sector_trend = self._handle_write(value, error)

    @property
    def calendar_events_earnings(self):
        return self._handle_read(self.__calendar_events_earnings)

    @calendar_events_earnings.setter
    def calendar_events_earnings(self, value, error=None):
        self.__calendar_events_earnings = self._handle_write(value, error)

    @property
    def calendar_events_dividends(self):
        return self._handle_read(self.__calendar_events_dividends)

    @calendar_events_dividends.setter
    def calendar_events_dividends(self, value, error=None):
        self.__calendar_events_dividends = self._handle_write(value, error)

    @property
    def sec_filings(self):
        return self._handle_read(self.__sec_filings)

    @sec_filings.setter
    def sec_filings(self, value, error=None):
        self.__sec_filings = self._handle_write(value, error)

    @property
    def upgrade_downgrade_history(self):
        return self._handle_read(self.__upgrade_downgrade_history)

    @upgrade_downgrade_history.setter
    def upgrade_downgrade_history(self, value, error=None):
        self.__upgrade_downgrade_history = self._handle_write(value, error)

    @property
    def net_share_purchase_activity(self):
        return self._handle_read(self.__net_share_purchase_activity)

    @net_share_purchase_activity.setter
    def net_share_purchase_activity(self, value, error=None):
        self.__net_share_purchase_activity = self._handle_write(value, error)
