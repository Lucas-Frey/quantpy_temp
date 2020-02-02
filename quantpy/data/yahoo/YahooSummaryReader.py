from quantpy.data.base.BaseReader import BaseReader
import pandas as pd
from collections import MutableMapping

import quantpy.data.yahoo.YahooExceptions as YahooExceptions
import re
import warnings


class YahooSummaryReader(BaseReader):

    def __init__(self, symbols,
                 include_asset_profile=False,
                 include_income_statement_history=False,
                 include_income_statement_history_quarterly=False,
                 include_balance_sheet_history=False,
                 include_balance_sheet_history_quarterly=False,
                 include_cash_flow_statement_history=False,
                 include_cash_flow_statement_history_quarterly=False,
                 include_earnings=False,
                 include_earnings_history=False,
                 include_financial_data=False,
                 include_default_key_statistics=False,
                 include_institution_ownership=False,
                 include_insider_holders=False,
                 include_insider_transactions=False,
                 include_fund_ownership=False,
                 include_major_direct_holders=False,
                 include_major_direct_holders_breakdown=False,
                 include_recommendation_trend=False,
                 include_earnings_trend=False,
                 include_industry_trend=False,
                 include_index_trend=False,
                 include_sector_trend=False,
                 include_calendar_events=False,
                 include_sec_filings=False,
                 include_upgrade_downgrade_history=False,
                 include_net_share_purchase_activity=False,
                 include_all=False,
                 retry_count=3, pause=0.1, timeout=5):

        self.__include_asset_profile = include_all or include_asset_profile
        self.__include_income_statement_history = include_all or include_income_statement_history
        self.__include_income_statement_history_quarterly = include_all or include_income_statement_history_quarterly
        self.__include_balance_sheet_history = include_all or include_balance_sheet_history
        self.__include_balance_sheet_history_quarterly = include_all or include_balance_sheet_history_quarterly
        self.__include_cash_flow_statement_history = include_all or include_cash_flow_statement_history
        self.__include_cash_flow_statement_history_quarterly = include_all or include_cash_flow_statement_history_quarterly
        self.__include_earnings = include_earnings or include_all
        self.__include_earnings_history = include_all or include_earnings_history
        self.__include_financial_data = include_all or include_financial_data
        self.__include_default_key_statistics = include_all or include_default_key_statistics

        self.__include_institution_ownership = include_all or include_institution_ownership
        self.__include_insider_holders = include_all or include_insider_holders
        self.__include_insider_transactions = include_all or include_insider_transactions
        self.__include_fund_ownership = include_all or include_fund_ownership
        self.__include_major_direct_holders = include_all or include_major_direct_holders
        self.__include_major_direct_holders_breakdown = include_all or include_major_direct_holders_breakdown

        self.__include_recommendation_trend = include_all or include_recommendation_trend
        self.__include_earnings_trend = include_all or include_earnings_trend
        self.__include_industry_trend = include_all or include_industry_trend
        self.__include_index_trend = include_all or include_index_trend
        self.__include_sector_trend = include_all or include_sector_trend

        self.__include_calendar_events = include_all or include_calendar_events
        self.__include_sec_filings = include_all or include_sec_filings
        self.__include_upgrade_downgrade_history = include_all or include_upgrade_downgrade_history
        self.__include_net_share_purchase_activity = include_all or include_net_share_purchase_activity

        self.__include_all = include_all

        self.__pep_pattern = re.compile(r'(?<!^)(?=[A-Z])')

        # Call the super class' constructor.
        super().__init__(symbols=symbols, retry_count=retry_count, pause=pause,
                         timeout=timeout)

    @property
    def _url(self):
        """
        Method to get the url of the API endpoint for Yahoo! Finance.
        """

        return 'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{}'

    @property
    def _params(self):
        """
        Method to get the parameters for the API endpoint.
        :return: A dictionary of parameters. This is called after sanitization.
        :rtype dict
        """
        modules_list = ''

        if self.__include_asset_profile:
            modules_list += 'assetProfile,'
        if self.__include_income_statement_history:
            modules_list += 'incomeStatementHistory,'
        if self.__include_income_statement_history_quarterly:
            modules_list += 'incomeStatementHistoryQuarterly,'
        if self.__include_balance_sheet_history:
            modules_list += 'balanceSheetHistory,'
        if self.__include_balance_sheet_history_quarterly:
            modules_list += 'balanceSheetHistoryQuarterly,'
        if self.__include_cash_flow_statement_history:
            modules_list += 'cashFlowStatementHistory,'
        if self.__include_cash_flow_statement_history_quarterly:
            modules_list += 'cashFlowStatementHistoryQuarterly,'
        if self.__include_earnings:
            modules_list += 'earnings,'
        if self.__include_earnings_history:
            modules_list += 'earningsHistory,'
        if self.__include_financial_data:
            modules_list += 'financialData,'
        if self.__include_default_key_statistics:
            modules_list += 'defaultKeyStatistics,'

        if self.__include_institution_ownership:
            modules_list += 'institutionOwnership,'
        if self.__include_insider_holders:
            modules_list += 'insiderHolders,'
        if self.__include_insider_transactions:
            modules_list += 'insiderTransactions,'
        if self.__include_fund_ownership:
            modules_list += 'fundOwnership,'
        if self.__include_major_direct_holders:
            modules_list += 'majorDirectHolders,'
        if self.__include_major_direct_holders_breakdown:
            modules_list += 'majorHoldersBreakdown,'

        if self.__include_recommendation_trend:
            modules_list += 'recommendationTrend,'
        if self.__include_earnings_trend:
            modules_list += 'earningsTrend,'
        if self.__include_industry_trend:
            modules_list += 'industryTrend,'
        if self.__include_index_trend:
            modules_list += 'indexTrend,'
        if self.__include_sector_trend:
            modules_list += 'sectorTrend,'

        if self.__include_calendar_events:
            modules_list += 'calendarEvents,'
        if self.__include_sec_filings:
            modules_list += 'secFilings,'
        if self.__include_upgrade_downgrade_history:
            modules_list += 'upgradeDowngradeHistory,'
        if self.__include_net_share_purchase_activity:
            modules_list += 'netSharePurchaseActivity,'

        return {'modules': modules_list}

    def _check_init_args(self):
        if not (self.__include_asset_profile or
                self.__include_income_statement_history or
                self.__include_income_statement_history_quarterly or
                self.__include_balance_sheet_history or
                self.__include_balance_sheet_history_quarterly or
                self.__include_cash_flow_statement_history or
                self.__include_cash_flow_statement_history_quarterly or
                self.__include_earnings or
                self.__include_earnings_history or
                self.__include_financial_data or
                self.__include_default_key_statistics or
                self.__include_institution_ownership or
                self.__include_insider_holders or
                self.__include_insider_transactions or
                self.__include_fund_ownership or
                self.__include_major_direct_holders or
                self.__include_major_direct_holders_breakdown or
                self.__include_recommendation_trend or
                self.__include_earnings_trend or
                self.__include_industry_trend or
                self.__include_index_trend or
                self.__include_sector_trend or
                self.__include_calendar_events or
                self.__include_sec_filings or
                self.__include_upgrade_downgrade_history or
                self.__include_net_share_purchase_activity):
            raise ValueError('Did not specify any summary values to get.')

    def _check_data(self, symbol, data=None):

        if 'Will be right back' in data.text:
            return YahooSummaryReader.YahooSummary(symbol,
                                                   exception=YahooExceptions.YahooRuntimeError('Yahoo Finance is currently down.'))
        else:
            data_json = data.json()
            if data_json['quoteSummary']['result'] is None and data_json['quoteSummary']['error'] is not None:
                return YahooSummaryReader.YahooSummary(symbol,
                                                       exception=data_json['quoteSummary']['error']['description'])
            else:
                return YahooSummaryReader.YahooSummary(symbol,
                                                       exception=data_json)

    def _parse_data(self, symbol, data):
        modules = data['quoteSummary']['result'][0]
        ys = YahooSummaryReader.YahooSummary(symbol)

        if self.__include_asset_profile:
            try:
                ys.profile, ys.company_officers = self.__parse_asset_profile_module(modules, 'assetProfile')
            except Exception as e:
                ys.profile = None, e
                ys.company_officers = None, e

        if self.__include_income_statement_history:
            try:
                ys.income_statement_history = self.__parse_module(modules, 'incomeStatementHistory', 'incomeStatementHistory')
            except Exception as e:
                ys.income_statement_history = None, e

        if self.__include_income_statement_history_quarterly:
            try:
                ys.income_statement_history_quarterly = self.__parse_module(modules, 'incomeStatementHistoryQuarterly', 'incomeStatementHistory')
            except Exception as e:
                ys.income_statement_history = None, e

        if self.__include_balance_sheet_history:
            try:
                ys.balance_sheet_history = self.__parse_module(modules, 'balanceSheetHistory', 'balanceSheetStatements')
            except Exception as e:
                ys.balance_sheet_history = None, e

        if self.__include_balance_sheet_history_quarterly:
            try:
                ys.balance_sheet_history_quarterly = self.__parse_module(modules, 'balanceSheetHistoryQuarterly', 'balanceSheetStatements')
            except Exception as e:
                ys.balance_sheet_history_quarterly = None, e

        if self.__include_cash_flow_statement_history:
            try:
                ys.cash_flow_statement_history = self.__parse_module(modules, 'cashflowStatementHistory', 'cashflowStatements')
            except Exception as e:
                ys.cash_flow_statement_history = None, e

        if self.__include_cash_flow_statement_history_quarterly:
            try:
                ys.cash_flow_statement_history_quarterly = self.__parse_module(modules, 'cashflowStatementHistoryQuarterly', 'cashflowStatements')
            except Exception as e:
                ys.cash_flow_statement_history_quarterly = None, e

        if self.__include_earnings:
            try:
                ys.earnings_estimates, ys.earnings_estimates_quarterly, ys.financials_quarterly, ys.financials_yearly = self.__parse_earnings_module(modules, 'earnings')
            except Exception as e:
                ys.earnings_estimates = None, e
                ys.earnings_estimates_quarterly = None, e
                ys.financials_quarterly = None, e
                ys.financials_yearly = None, e

        if self.__include_earnings_history:
            try:
                ys.earnings_history = self.__parse_module(modules, 'earningsHistory', 'history')
            except Exception as e:
                ys.earnings_history = None, e

        if self.__include_financial_data:
            try:
                ys.financial_data = self.__parse_module(modules, 'financialData')
            except Exception as e:
                ys.financial_data = None, e

        if self.__include_default_key_statistics:
            try:
                ys.default_key_statistics = self.__parse_module(modules, 'defaultKeyStatistics')
            except Exception as e:
                ys.default_key_statistics = None, e

        if self.__include_institution_ownership:
            try:
                ys.institution_ownership = self.__parse_module(modules, 'institutionOwnership', 'ownershipList')
            except Exception as e:
                ys.institution_ownership = None, e

        if self.__include_insider_holders:
            try:
                ys.insider_holders = self.__parse_module(modules, 'insiderHolders', 'holders')
            except Exception as e:
                ys.insider_holders = None, e

        if self.__include_insider_transactions:
            try:
                ys.insider_transactions = self.__parse_module(modules, 'insiderTransactions', 'transactions')
            except Exception as e:
                ys.insider_transactions = None, e

        if self.__include_fund_ownership:
            try:
                ys.fund_ownership = self.__parse_module(modules, 'fundOwnership', 'ownershipList')
            except Exception as e:
                ys.fund_ownership = None, e

        if self.__include_major_direct_holders:
            try:
                ys.major_direct_holders = self.__parse_module(modules, 'majorDirectHolders', 'holders')
            except Exception as e:
                ys.major_direct_holders = None, e

        if self.__include_major_direct_holders_breakdown:
            try:
                ys.major_direct_holders_breakdown = self.__parse_module(modules, 'majorHoldersBreakdown')
            except Exception as e:
                ys.major_direct_holders_breakdown = None, e

        if self.__include_recommendation_trend:
            try:
                ys.recommendation_trend = self.__parse_module(modules, 'recommendationTrend', 'trend')
            except Exception as e:
                ys.recommendation_trend = None, e

        if self.__include_earnings_trend:
            try:
                ys.earnings_trend = self.__parse_module(modules, 'earningsTrend', 'trend')
            except Exception as e:
                ys.earnings_trend = None, e

        if self.__include_industry_trend:
            try:
                ys.industry_trend = self.__parse_module(modules, 'industryTrend')
            except Exception as e:
                ys.industry_trend = None, e

        if self.__include_index_trend:
            try:
                ys.index_trend_info, ys.index_trend_estimate = self.__parse_index_trend_module(modules, 'indexTrend')
            except Exception as e:
                ys.index_trend = None, e

        if self.__include_sector_trend:
            try:
                ys.sector_trend = self.__parse_module(modules, 'sectorTrend')
            except Exception as e:
                ys.sector_trend = None, e

        if self.__include_calendar_events:
            try:
                ys.calendar_events_earnings, ys.calendar_events_dividends = self.__parse_calendar_events_module(modules, 'calendarEvents')
            except Exception as e:
                ys.calendar_events_earnings = None, e
                ys.calendar_events_dividends = None, e

        if self.__include_sec_filings:
            try:
                ys.sec_filings = self.__parse_module(modules, 'secFilings', 'filings')
            except Exception as e:
                ys.sec_filings = None, e

        if self.__include_upgrade_downgrade_history:
            try:
                ys.upgrade_downgrade_history = self.__parse_module(modules, 'upgradeDowngradeHistory', 'history')
            except Exception as e:
                ys.upgrade_downgrade_history = None, e

        if self.__include_net_share_purchase_activity:
            try:
                ys.net_share_purchase_activity = self.__parse_module(modules, 'netSharePurchaseActivity')
            except Exception as e:
                ys.net_share_purchase_activity = None, e

        return ys

    def __parse_module(self, modules_dict, module_name, submodule_name=None):
        if not submodule_name:
            module = modules_dict[module_name]
        else:
            module = modules_dict[module_name][submodule_name]

        module_dataframe = self.__format_dataframe(module)

        return module_dataframe

    def __format_dataframe(self, data_dictionary):
        if isinstance(data_dictionary, list):
            module = [self.flatten(data) for data in data_dictionary]
            module = pd.DataFrame(module)
        else:
            module = self.flatten(data_dictionary)
            module = pd.DataFrame([module])

        module_columns = [column for column in module.columns
                          if not ('.fmt' in column or '.longFmt' in column)]
        module = module[module_columns]

        new_columns_dict = {col: self.__pep_pattern.sub('_', col.split('.')[0]).lower() for col in
                            module.columns}
        module.rename(columns=new_columns_dict, inplace=True)

        return module

    def __parse_asset_profile_module(self, results_dict, module_name):
        try:
            profile_dict = results_dict[module_name]
            company_officers_dict = profile_dict['companyOfficers']
            del profile_dict['companyOfficers']

            profile = self.__format_dataframe(profile_dict)
            company_officers = self.__format_dataframe(company_officers_dict)

            return profile, company_officers

        except Exception as e:
            return e, e

    def __parse_earnings_module(self, results_dict, module_name):
        try:
            module = results_dict[module_name]

            earnings_estimates = module['earningsChart']
            earnings_quarterly = earnings_estimates['quarterly']
            del earnings_estimates['quarterly']

            if isinstance(earnings_estimates['earningsDate'], list):
                earnings_estimates['earningsDate'] = earnings_estimates['earningsDate'][0]

            earnings_estimates = self.__format_dataframe(earnings_estimates)
            earnings_quarterly = self.__format_dataframe(earnings_quarterly)

            financial_yearly = module['financialsChart']['yearly']
            financial_quarterly = module['financialsChart']['quarterly']

            financial_yearly = self.__format_dataframe(financial_yearly)
            financial_quarterly = self.__format_dataframe(financial_quarterly)

            return earnings_estimates, earnings_quarterly, financial_quarterly, financial_yearly

        except Exception as e:
            return e, e, e, e

    def __parse_index_trend_module(self, results_dict, module_name):
        try:
            index_trend_info = results_dict[module_name]
            index_trend_estimates = index_trend_info['estimates']
            del index_trend_info['estimates']

            index_trend_info = self.__format_dataframe(index_trend_info)
            index_trend_estimates = self.__format_dataframe(index_trend_estimates)

            return index_trend_info, index_trend_estimates

        except Exception as e:
            return e, e

    def __parse_calendar_events_module(self, results_dict, module_name):
        try:
            calender_events_earnings = results_dict[module_name]['earnings']

            if isinstance(calender_events_earnings['earningsDate'], list):
                calender_events_earnings['earningsDate'] = calender_events_earnings['earningsDate'][0]

            calender_events_earnings = self.__format_dataframe(calender_events_earnings)

            dividends = results_dict[module_name]
            del dividends['earnings']

            calendar_events_dividends = self.__format_dataframe(dividends)

            return calender_events_earnings, calendar_events_dividends

        except Exception as e:
            return e, e

    def _handle_read_exception(self, symbol, exception):
        return YahooSummaryReader.YahooSummary(symbol, exception)

    def flatten(self, d, parent_key='', sep='.'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    class YahooSummary:

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

            self.__symbol = symbol
            self.__exception = exception

        @property
        def symbol(self):
            return self.__symbol

        @property
        def exception(self):
            return self.__exception

        @property
        def profile(self):
            return self._handle_read_summary(self.__profile)

        @profile.setter
        def profile(self, value=None, error=None):
            self.__profile = self._handle_write_summary(value, error)

        @property
        def company_officers(self):
            return self._handle_read_summary(self.__company_officers)

        @company_officers.setter
        def company_officers(self, value, error=None):
            self.__company_officers = self._handle_write_summary(value, error)

        @property
        def income_statement_history(self):
            return self._handle_read_summary(self.__income_statement_history)

        @income_statement_history.setter
        def income_statement_history(self, value, error=None):
            self.__income_statement_history = self._handle_write_summary(value, error)

        @property
        def income_statement_history_quarterly(self):
            return self._handle_read_summary(self.__income_statement_history_quarterly)

        @income_statement_history_quarterly.setter
        def income_statement_history_quarterly(self, value, error=None):
            self.__income_statement_history_quarterly = self._handle_write_summary(value, error)

        @property
        def balance_sheet_history(self):
            return self._handle_read_summary(self.__balance_sheet_history)

        @balance_sheet_history.setter
        def balance_sheet_history(self, value, error=None):
            self.__balance_sheet_history = self._handle_write_summary(value, error)

        @property
        def balance_sheet_history_quarterly(self):
            return self._handle_read_summary(self.__balance_sheet_history_quarterly)

        @balance_sheet_history_quarterly.setter
        def balance_sheet_history_quarterly(self, value, error=None):
            self.__balance_sheet_history_quarterly = self._handle_write_summary(value, error)

        @property
        def cash_flow_statement_history(self):
            return self._handle_read_summary(self.__cash_flow_statement_history)

        @cash_flow_statement_history.setter
        def cash_flow_statement_history(self, value, error=None):
            self.__cash_flow_statement_history = self._handle_write_summary(value, error)

        @property
        def cash_flow_statement_history_quarterly(self):
            return self._handle_read_summary(self.__cash_flow_statement_history_quarterly)

        @cash_flow_statement_history_quarterly.setter
        def cash_flow_statement_history_quarterly(self, value, error=None):
            self.__cash_flow_statement_history_quarterly = self._handle_write_summary(value, error)

        @property
        def earnings_estimates(self):
            return self._handle_read_summary(self.__earnings_estimates)

        @earnings_estimates.setter
        def earnings_estimates(self, value, error=None):
            self.__earnings_estimates = self._handle_write_summary(value, error)

        @property
        def earnings_estimates_quarterly(self):
            return self._handle_read_summary(self.__earnings_estimates_quarterly)

        @earnings_estimates_quarterly.setter
        def earnings_estimates_quarterly(self, value, error=None):
            self.__earnings_estimates_quarterly = self._handle_write_summary(value, error)

        @property
        def financials_quarterly(self):
            return self._handle_read_summary(self.__financials_quarterly)

        @financials_quarterly.setter
        def financials_quarterly(self, value, error=None):
            self.__financials_quarterly = self._handle_write_summary(value, error)

        @property
        def financials_yearly(self):
            return self._handle_read_summary(self.__financials_yearly)

        @financials_yearly.setter
        def financials_yearly(self, value, error=None):
            self.__financials_yearly = self._handle_write_summary(value, error)

        @property
        def earnings_history(self):
            return self._handle_read_summary(self.__earnings_history)

        @earnings_history.setter
        def earnings_history(self, value, error=None):
            self.__earnings_history = self._handle_write_summary(value, error)

        @property
        def financial_data(self):
            return self._handle_read_summary(self.__financial_data)

        @financial_data.setter
        def financial_data(self, value, error=None):
            self.__financial_data = self._handle_write_summary(value, error)

        @property
        def default_key_statistics(self):
            return self._handle_read_summary(self.__default_key_statistics)

        @default_key_statistics.setter
        def default_key_statistics(self, value, error=None):
            self.__default_key_statistics = self._handle_write_summary(value, error)

        @property
        def institution_ownership(self):
            return self._handle_read_summary(self.__institution_ownership)

        @institution_ownership.setter
        def institution_ownership(self, value, error=None):
            self.__institution_ownership = self._handle_write_summary(value, error)

        @property
        def insider_holders(self):
            return self._handle_read_summary(self.__insider_holders)

        @insider_holders.setter
        def insider_holders(self, value, error=None):
            self.__insider_holders = self._handle_write_summary(value, error)

        @property
        def insider_transactions(self):
            return self._handle_read_summary(self.__insider_transactions)

        @insider_transactions.setter
        def insider_transactions(self, value, error=None):
            self.__insider_transactions = self._handle_write_summary(value, error)

        @property
        def fund_ownership(self):
            return self._handle_read_summary(self.__fund_ownership)

        @fund_ownership.setter
        def fund_ownership(self, value, error=None):
            self.__fund_ownership = self._handle_write_summary(value, error)

        @property
        def major_direct_holders(self):
            return self._handle_read_summary(self.__major_direct_holders)

        @major_direct_holders.setter
        def major_direct_holders(self, value, error=None):
            self.__major_direct_holders = self._handle_write_summary(value, error)

        @property
        def major_direct_holders_breakdown(self):
            return self._handle_read_summary(self.__major_direct_holders_breakdown)

        @major_direct_holders_breakdown.setter
        def major_direct_holders_breakdown(self, value, error=None):
            self.__major_direct_holders_breakdown = self._handle_write_summary(value, error)

        @property
        def recommendation_trend(self):
            return self._handle_read_summary(self.__recommendation_trend)

        @recommendation_trend.setter
        def recommendation_trend(self, value, error=None):
            self.__recommendation_trend = self._handle_write_summary(value, error)

        @property
        def earnings_trend(self):
            return self._handle_read_summary(self.__earnings_trend)

        @earnings_trend.setter
        def earnings_trend(self, value, error=None):
            self.__earnings_trend = self._handle_write_summary(value, error)

        @property
        def industry_trend(self):
            return self._handle_read_summary(self.__industry_trend)

        @industry_trend.setter
        def industry_trend(self, value, error=None):
            self.__industry_trend = self._handle_write_summary(value, error)

        @property
        def index_trend_info(self):
            return self._handle_read_summary(self.__index_trend_info)

        @index_trend_info.setter
        def index_trend_info(self, value, error=None):
            self.__index_trend_info = self._handle_write_summary(value, error)

        @property
        def index_trend_estimate(self):
            return self._handle_read_summary(self.__index_trend_estimate)

        @index_trend_estimate.setter
        def index_trend_estimate(self, value, error=None):
            self.__index_trend_estimate = self._handle_write_summary(value, error)

        @property
        def sector_trend(self):
            return self._handle_read_summary(self.__sector_trend)

        @sector_trend.setter
        def sector_trend(self, value, error=None):
            self.__sector_trend = self._handle_write_summary(value, error)

        @property
        def calendar_events_earnings(self):
            return self._handle_read_summary(self.__calendar_events_earnings)

        @calendar_events_earnings.setter
        def calendar_events_earnings(self, value, error=None):
            self.__calendar_events_earnings = self._handle_write_summary(value, error)

        @property
        def calendar_events_dividends(self):
            return self._handle_read_summary(self.__calendar_events_dividends)

        @calendar_events_dividends.setter
        def calendar_events_dividends(self, value, error=None):
            self.__calendar_events_dividends = self._handle_write_summary(value, error)

        @property
        def sec_filings(self):
            return self._handle_read_summary(self.__sec_filings)

        @sec_filings.setter
        def sec_filings(self, value, error=None):
            self.__sec_filings = self._handle_write_summary(value, error)

        @property
        def upgrade_downgrade_history(self):
            return self._handle_read_summary(self.__upgrade_downgrade_history)

        @upgrade_downgrade_history.setter
        def upgrade_downgrade_history(self, value, error=None):
            self.__upgrade_downgrade_history = self._handle_write_summary(value, error)

        @property
        def net_share_purchase_activity(self):
            return self._handle_read_summary(self.__net_share_purchase_activity)

        @net_share_purchase_activity.setter
        def net_share_purchase_activity(self, value, error=None):
            self.__net_share_purchase_activity = self._handle_write_summary(value, error)

        def _handle_read_summary(self, summary_object):
            if self.__exception is None:
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
                warnings.warn(str(self.__exception))
                return None

        def _handle_write_summary(self, value, error):
            summary_object = YahooSummaryReader.YahooSummary.SummaryObject()

            if value is not None and error is not None:
                raise ValueError('Cannot assign both a value and an error.')
            else:
                if value is not None:
                    summary_object.value = value
                else:
                    summary_object.error = error

            return summary_object
