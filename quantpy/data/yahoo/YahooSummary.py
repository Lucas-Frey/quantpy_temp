import warnings

class YahooSummary:

    class SummaryObject:
        def __init__(self):
            self._included = False
            self._value = None
            self._error_occurred = False
            self._error = None

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

    def __init__(self, symbol):
        self._profile = None
        self._company_officers = None
        self._company_officers_error = None
        self._income_statement_history = None
        self._income_statement_history_error = None
        self._income_statement_history_quarterly = None
        self._income_statement_history_quarterly_error = None
        self._balance_sheet_history = None
        self._balance_sheet_history_error = None
        self._balance_sheet_history_quarterly = None
        self._balance_sheet_history_quarterly_error = None
        self._cash_flow_statement_history = None
        self._cash_flow_statement_history_error = None
        self._cash_flow_statement_history_quarterly = None
        self._cash_flow_statement_history_quarterly_error = None
        self._earnings_estimates = None
        self._earnings_estimates_error = None
        self._earnings_estimates_quarterly = None
        self._earnings_estimates_quarterly_error = None
        self._financials_quarterly = None
        self._financials_quarterly_error = None
        self._financials_yearly = None
        self._financials_yearly_error = None
        self._earnings_history = None
        self._earnings_history_error = None
        self._financial_data = None
        self._financial_data_error = None
        self._default_key_statistics = None
        self._default_key_statistics_error = None

        self._institution_ownership = None
        self._insider_holders = None
        self._insider_transactions = None
        self._fund_ownership = None
        self._major_direct_holders = None
        self._major_direct_holders_breakdown = None

        self._recommendation_trend = None
        self._earnings_trend = None
        self._industry_trend = None
        self._index_trend_info = None
        self._index_trend_estimate = None
        self._sector_trend = None

        self._calendar_events_earnings = None
        self._calendar_events_dividends = None
        self._sec_filings = None
        self._upgrade_downgrade_history = None
        self._net_share_purchase_activity = None

        self.symbol = symbol

    @property
    def profile(self):
        if self._profile:
            return self._profile
        else:
            return self._handle_none_value(self._profile, self._included_profile)

    @profile.setter
    def profile(self, value):
        self._included_profile = True
        self._profile = value

    @property
    def company_officers(self):
        if self._company_officers:
            return self._company_officers
        else:
            if self._included_company_officers:
                return Exception(self._company_officers)
            else:
                warnings.warn('Company officers referenced and not set.')
                return None
            return self._handle_none_value(,)

    @company_officers.setter
    def company_officers(self, value, error=None):
        self._included_company_officers = True
        self._company_officers = value

    @property
    def income_statement_history(self):
        if self._income_statement_history:
            return self._income_statement_history
        else:
            if self._included_income_statement_history:
                raise Exception(self._income_statement_history)
            else:
                warnings.warn('Income statement history referenced and not set.')
                return None
            return self._handle_none_value(,)

    @income_statement_history.setter
    def income_statement_history(self, value):
        self._included_income_statement_history = True
        self._income_statement_history = value

    @property
    def income_statement_history_quarterly(self):
        if self._income_statement_history_quarterly:
            return self._income_statement_history_quarterly
        else:
            if self._included_balance_sheet_history_quarterly:
                raise Exception(self._income_statement_history_quarterly)
            else:
                warnings.warn('Quarterly income statement history not set.')
                return None
            return self._handle_none_value(,)

    @income_statement_history_quarterly.setter
    def income_statement_history_quarterly(self, value):
        self._included_income_statement_history_quarterly = True
        self._income_statement_history_quarterly = value

    @property
    def balance_sheet_history(self):
        if self._balance_sheet_history:
            return self._balance_sheet_history
        else:
            if self._included_balance_sheet_history:
                raise Exception(self._balance_sheet_history)
            else:
                warnings.warn('Balance sheet history referenced and not set.')
                return None
            return self._handle_none_value(,)

    @balance_sheet_history.setter
    def balance_sheet_history(self, value):
        self._included_balance_sheet_history = True
        self._balance_sheet_history = value

    @property
    def balance_sheet_history_quarterly(self):
        if self._balance_sheet_history_quarterly:
            return self._balance_sheet_history_quarterly
        else:
            if self._included_balance_sheet_history_quarterly:
                raise Exception(self._balance_sheet_history_quarterly)
            else:
                warnings.warn('Quarterly balance sheet history referenced and not set.')
                return None
            return self._handle_none_value(,)

    @balance_sheet_history_quarterly.setter
    def balance_sheet_history_quarterly(self, value):
        self._included_balance_sheet_history_quarterly = True
        self._balance_sheet_history_quarterly = value

    @property
    def cash_flow_statement_history(self):
        if self._cash_flow_statement_history:
            return self._cash_flow_statement_history
        else:
            if self._included_cash_flow_statement_history:
                raise Exception(self._cash_flow_statement_history)
            else:
                warnings.warn('Cash flow history referenced and not set.')
                return None
            return self._handle_none_value(,)

    @cash_flow_statement_history.setter
    def cash_flow_statement_history(self, value):
        self._included_cash_flow_statement_history = True
        self._cash_flow_statement_history = value

    @property
    def cash_flow_statement_history_quarterly(self):
        if self._cash_flow_statement_history_quarterly:
            return self._cash_flow_statement_history_quarterly
        else:
            if self._included_balance_sheet_history_quarterly:
                raise Exception(self._cash_flow_statement_history_quarterly)
            else:
                warnings.warn('Quarterly cash flow history referenced and not set.')
                return None
            return self._handle_none_value(,)

    @cash_flow_statement_history_quarterly.setter
    def cash_flow_statement_history_quarterly(self, value):
        self._included_cash_flow_statement_history_quarterly = True
        self._cash_flow_statement_history_quarterly = value

    @property
    def earnings_estimates(self):
        if self._earnings_estimates:
            return self._earnings_estimates
        else:
            if self._included_earnings_estimates:
                raise Exception(self._earnings_estimates)
            else:
                warnings.warn('Earnings estimates referenced and not set.')
                return None
            return self._handle_none_value(,)

    @earnings_estimates.setter
    def earnings_estimates(self, value):
        self._included_earnings_estimates = True
        self._earnings_estimates = value

    @property
    def earnings_estimates_quarterly(self):
        if self._earnings_estimates_quarterly:
            return self._earnings_estimates_quarterly
        else:
            if self._included_earnings_estimates_quarterly:
                raise Exception(self._earnings_estimates_quarterly)
            else:
                warnings.warn('Quarterly earnings estimates referenced and not set.')
                return None
            return self._handle_none_value(,)

    @earnings_estimates_quarterly.setter
    def earnings_estimates_quarterly(self, value):
        self._included_earnings_estimates_quarterly = True
        self._earnings_estimates_quarterly = value

    @property
    def financials_quarterly(self):
        if self._financials_quarterly:
            return self._financials_quarterly
        else:
            if self._included_financials_quarterly:
                raise Exception(self._financials_quarterly)
            else:
                warnings.warn('Quarterly financials referenced and not set.')
                return None
            return self._handle_none_value(,)

    @financials_quarterly.setter
    def financials_quarterly(self, value):
        self._included_financials_quarterly = True
        self._financials_quarterly = value

    @property
    def financials_yearly(self):
        if self._financials_yearly:
            return self._financials_yearly
        else:
            if self._included_financials_yearly:
                raise Exception(self._financials_yearly)
            else:
                warnings.warn('Yearly financials referenced and not set.')
                return None
            return self._handle_none_value(,)

    @financials_yearly.setter
    def financials_yearly(self, value):
        self._included_financials_yearly = True
        self._financials_yearly = value

    @property
    def earnings_history(self):
        if self._earnings_history:
            return self._earnings_history
        else:
            if self._included_earnings_history:
                raise Exception(self._earnings_history)
            else:
                warnings.warn('Earnings history referenced and not set.')
                return None
            return self._handle_none_value(,)

    @earnings_history.setter
    def earnings_history(self, value):
        self._included_earnings_history = True
        self._earnings_history = value

    @property
    def financial_data(self):
        if self._financial_data:
            return self._financial_data
        else:
            if self._included_financial_data:
                raise Exception(self.financial_data)
            else:
                warnings.warn('Financial data referenced and not set.')
                return None
            return self._handle_none_value(,)

    @financial_data.setter
    def financial_data(self, value):
        self._included_financial_data = True
        self._financial_data = value

    @property
    def default_key_statistics(self):
        if self._default_key_statistics:
            return self._default_key_statistics
        else:
            if self._included_default_key_statistics:
                raise Exception(self._default_key_statistics)
            else:
                warnings.warn('Default key statistics referenced and not set.')
                return None
            return self._handle_none_value(,)

    @default_key_statistics.setter
    def default_key_statistics(self, value):
        self._included_default_key_statistics = True
        self._default_key_statistics = value

    @property
    def institution_ownership(self):
        if self._institution_ownership:
            return self._institution_ownership
        else:
            if self._included_institution_ownership:
                raise Exception(self._institution_ownership)
            else:
                warnings.warn('Institutional ownership referenced and not set.')
                return None
            return self._handle_none_value(,)

    @institution_ownership.setter
    def institution_ownership(self, value):
        self._included_institution_ownership = True
        self._institution_ownership = value

    @property
    def insider_holders(self):
        if self._insider_holders:
            return self._insider_holders
        else:
            if self._included_insider_holders:
                raise Exception(self._insider_holders)
            else:
                warnings.warn('Insider holders referenced and not set.')
                return None
            return self._handle_none_value(,)

    @insider_holders.setter
    def insider_holders(self, value):
        self._included_insider_holders = True
        self._insider_holders = value

    @property
    def insider_transactions(self):
        if self._insider_transactions:
            return self._insider_transactions
        else:
            if self._included_insider_transactions:
                raise Exception(self._insider_transactions)
            else:
                warnings.warn('Insider transactions referenced and not set.')
                return None
            return self._handle_none_value(,)

    @insider_transactions.setter
    def insider_transactions(self, value):
        self._included_insider_transactions = True
        self._insider_transactions = value

    @property
    def fund_ownership(self):
        if self._fund_ownership:
            return self._fund_ownership
        else:
            if self._included_fund_ownership:
                raise Exception(self._fund_ownership)
            else:
                warnings.warn('Fund ownership referenced not set.')
                return None
            return self._handle_none_value(,)

    @fund_ownership.setter
    def fund_ownership(self, value):
        self._included_fund_ownership = True
        self._fund_ownership = value

    @property
    def major_direct_holders(self):
        if self._major_direct_holders:
            return self._major_direct_holders
        else:
            if self._included_major_direct_holders:
                raise Exception(self._major_direct_holders)
            else:
                warnings.warn('Major direct holders referenced and not set.')
                return None
            return self._handle_none_value(,)

    @major_direct_holders.setter
    def major_direct_holders(self, value):
        self._included_major_direct_holders = True
        self.major_direct_holders = value

    @property
    def major_direct_holders_breakdown(self):
        if self._major_direct_holders_breakdown:
            return self._major_direct_holders_breakdown
        else:
            if self._included_major_direct_holders_breakdown:
                return self._major_direct_holders_breakdown
            else:
                raise ValueError('Major direct holders breakdown not set.')
            return self._handle_none_value(,)

    @major_direct_holders_breakdown.setter
    def major_direct_holders_breakdown(self, value):
        self._included_major_direct_holders_breakdown = True
        self._major_direct_holders_breakdown = value

    @property
    def recommendation_trend(self):
        if self._recommendation_trend:
            return self._recommendation_trend
        else:
            if self._included_recommendation_trend:
                return self._recommendation_trend
            else:
                raise ValueError('Recommendation trend not set.')
            return self._handle_none_value(,)

    @recommendation_trend.setter
    def recommendation_trend(self, value):
        self._included_recommendation_trend = True
        self._recommendation_trend = value

    @property
    def earnings_trend(self):
        if self._earnings_trend:
            return self._earnings_trend
        else:
            if self._included_earnings_trend:
                return self._earnings_trend
            else:
                raise ValueError('Earnings trend not set.')
            return self._handle_none_value(,)

    @earnings_trend.setter
    def earnings_trend(self, value):
        self._included_earnings_trend = True
        self._earnings_trend = value

    @property
    def industry_trend(self):
        if self._industry_trend:
            return self._industry_trend
        else:
            if self._included_industry_trend:
                return self._industry_trend
            else:
                raise ValueError('Industry trend not set.')
            return self._handle_none_value(,)

    @industry_trend.setter
    def industry_trend(self, value):
        self._included_industry_trend = True
        self._industry_trend = value

    @property
    def index_trend_info(self):
        if self._index_trend_info:
            return self._index_trend_info
        else:
            if self._included_index_trend_info:
                return self._index_trend_info
            else:
                raise ValueError('Index trend info not set.')
            return self._handle_none_value(,)

    @index_trend_info.setter
    def index_trend_info(self, value):
        self._included_index_trend_info = True
        self._index_trend_info = value

    @property
    def index_trend_estimate(self):
        if self._index_trend_estimate:
            return self._index_trend_estimate
        else:
            if self._included_index_trend_estimate:
                return self._index_trend_estimate
            else:
                raise ValueError('Index trend estimate not set.')
            return self._handle_none_value(,)

    @index_trend_estimate.setter
    def index_trend_estimate(self, value):
        self._included_index_trend_estimate = True
        self._index_trend_estimate = value

    @property
    def sector_trend(self):
        if self._sector_trend:
            return self._sector_trend
        else:
            if self._included_sector_trend:
                return self._sector_trend
            else:
                raise ValueError('Sector trend not set.')
            return self._handle_none_value(,)

    @sector_trend.setter
    def sector_trend(self, value):
        self._included_sector_trend = True
        self._sector_trend = value

    @property
    def calendar_events_earnings(self):
        if self._calendar_events_earnings:
            return self._calendar_events_earnings
        else:
            if self._included_calendar_events_earnings:
                return self._calendar_events_earnings
            else:
                raise ValueError('Calendar events earnings not set.')
            return self._handle_none_value(,)

    @calendar_events_earnings.setter
    def calendar_events_earnings(self, value):
        self._included_calendar_events_earnings = True
        self._calendar_events_earnings = value

    @property
    def calendar_events_dividends(self):
        if self._calendar_events_dividends:
            return self._calendar_events_dividends
        else:
            if self._included_calendar_events_dividends:
                return self._calendar_events_dividends
            else:
                raise ValueError('Calendar events dividends not set.')
            return self._handle_none_value(,)

    @calendar_events_dividends.setter
    def calendar_events_dividends(self, value):
        self._included_calendar_events_dividends = True
        self._calendar_events_dividends = value

    @property
    def sec_filings(self):
        if self._sec_filings:
            return self._sec_filings
        else:
            if self._included_sec_filings:
                return self._sec_filings
            else:
                raise ValueError('SEC filings not set.')
            return self._handle_none_value(,)

    @sec_filings.setter
    def sec_filings(self, value):
        self._included_sec_filings = True
        self._sec_filings = value

    @property
    def upgrade_downgrade_history(self):
        if self._upgrade_downgrade_history:
            return self._upgrade_downgrade_history
        else:
            if self._included_upgrade_downgrade_history:
                return self._upgrade_downgrade_history
            else:
                raise ValueError('Upgrade and downgrade history not set.')
            return self._handle_none_value(,)

    @upgrade_downgrade_history.setter
    def upgrade_downgrade_history(self, value):
        self._included_upgrade_downgrade_history = True
        self._upgrade_downgrade_history = value

    @property
    def net_share_purchase_activity(self):
        if self._net_share_purchase_activity:
            return self._net_share_purchase_activity
        else:
            if self._included_net_share_purchase_activity:
                return self._net_share_purchase_activity
            else:
                raise ValueError('Net purchase share activity not set.')
            return self._handle_none_value(,)

    @net_share_purchase_activity.setter
    def net_share_purchase_activity(self, value):
        self._included_net_share_purchase_activity = True
        self._net_share_purchase_activity = value

    def _handle_none_value(self, value, included_value):
        if included_value:
            raise Exception(value)
        else:
            warnings.warn('Value referenced and was never assigned.')
            return None
