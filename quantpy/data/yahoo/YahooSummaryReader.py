from quantpy.data.base.BaseReader import BaseReader
import pandas as pd
import collections

import quantpy.data.yahoo.YahooExceptions as YahooExceptions
import re


class YahooSummaryReader(BaseReader):

    def __init__(self, symbols, include_financials=False, include_holders=False,
                 include_trends=False, include_nonfinancials=False,
                 retry_count=3, pause=0.1, timeout=5):
        """
        Initializer function for the YahooHoldersReader class.
        :param symbols: The list of symbols to be used.
        :type symbols: str or list
        :param retry_count: Optional. The amount of times to retry an api call.
        :type retry_count int
        :param pause: Optional. The amount of time to pause between retries.
        :type pause float
        :param timeout: Optional. The amount of time until a request times out.
        :type timeout int
        """

        self._include_financials = include_financials
        self._include_holders = include_holders
        self._include_trends = include_trends
        self._include_nonfinancials = include_nonfinancials

        self._include_asset_profile = self._include_financials
        self._profile = None
        self._company_officers = None
        self._include_income_statement_history = self._include_financials
        self._income_statement_history = None
        self._include_income_statement_history_quarterly = self._include_financials
        self._income_statement_history_quarterly = None
        self._include_balance_sheet_history = self._include_financials
        self._balance_sheet_history = None
        self._include_balance_sheet_history_quarterly = self._include_financials
        self._balance_sheet_history_quarterly = None
        self._include_cash_flow_statement_history = self._include_financials
        self._cash_flow_statement_history = None
        self._include_cash_flow_statement_history_quarterly = self._include_financials
        self._cash_flow_statement_history_quarterly = None
        self._include_earnings = self._include_financials
        self._earnings_estimates = None
        self._earnings_quarterly = None
        self._financials_quarterly = None
        self._financials_yearly = None
        self._include_earnings_history = self._include_financials
        self._earnings_history = None
        self._include_financial_data = self._include_financials
        self._financial_data = None
        self._include_default_key_statistics = self._include_financials
        self._default_key_statistics = None

        self._include_institution_ownership = self._include_holders
        self._institution_ownership = None
        self._include_insider_holders = self._include_holders
        self._insider_holders = None
        self._include_insider_transactions = self._include_holders
        self._insider_transactions = None
        self._include_fund_ownership = self._include_holders
        self._fund_ownership = None
        self._include_major_direct_holders = self._include_holders
        self._major_direct_holders = None
        self._include_major_holders_breakdown = self._include_holders
        self._major_direct_holders_breakdown = None

        self._include_recommendation_trend = self._include_trends
        self._recommendation_trend = None
        self._include_earnings_trend = self._include_trends
        self._earnings_trend = None
        self._include_industry_trend = self._include_trends
        self._industry_trend = None
        self._include_index_trend = self._include_trends
        self._index_trend_info = None
        self._index_trend_estimate = None
        self._include_sector_trend = self._include_trends
        self._sector_trend = None

        self._include_calendar_events = self._include_nonfinancials
        self._calendar_events_earnings = None
        self._calendar_events_dividends = None
        self._include_sec_filings = self._include_nonfinancials
        self._sec_filings = None
        self._include_upgrade_downgrade_history = self._include_nonfinancials
        self._upgrade_downgrade_history = None
        self._include_net_share_purchase_activity = self._include_nonfinancials
        self._net_share_purchase_activity = None

        self.pep_pattern = re.compile(r'(?<!^)(?=[A-Z])')

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

        if self._include_financials:
            if self._include_asset_profile:
                modules_list += 'assetProfile,'
            if self._include_income_statement_history:
                modules_list += 'incomeStatementHistory,'
            if self._include_income_statement_history_quarterly:
                modules_list += 'incomeStatementHistoryQuarterly,'
            if self._include_balance_sheet_history:
                modules_list += 'balanceSheetHistory,'
            if self._include_balance_sheet_history_quarterly:
                modules_list += 'balanceSheetHistoryQuarterly,'
            if self._include_cash_flow_statement_history:
                modules_list += 'cashFlowStatementHistory,'
            if self._include_cash_flow_statement_history_quarterly:
                modules_list += 'cashFlowStatementHistoryQuarterly,'
            if self._include_earnings:
                modules_list += 'earnings,'
            if self._include_earnings_history:
                modules_list += 'earningsHistory,'
            if self._include_financial_data:
                modules_list += 'financialData,'
            if self._include_default_key_statistics:
                modules_list += 'defaultKeyStatistics,'

        if self._include_holders:
            if self._include_institution_ownership:
                modules_list += 'institutionOwnership,'
            if self._include_insider_holders:
                modules_list += 'insiderHolders,'
            if self._include_insider_transactions:
                modules_list += 'insiderTransactions,'
            if self._include_fund_ownership:
                modules_list += 'fundOwnership,'
            if self._include_major_direct_holders:
                modules_list += 'majorDirectHolders,'
            if self._include_major_holders_breakdown:
                modules_list += 'majorHoldersBreakdown,'

        if self._include_trends:
            if self._include_recommendation_trend:
                modules_list += 'recommendationTrend,'
            if self._include_earnings_trend:
                modules_list += 'earningsTrend,'
            if self._include_industry_trend:
                modules_list += 'industryTrend,'
            if self._include_index_trend:
                modules_list += 'indexTrend,'
            if self._include_sector_trend:
                modules_list += 'sectorTrend,'

        if self._include_nonfinancials:
            if self._include_calendar_events:
                modules_list += 'calendarEvents'
            if self._include_sec_filings:
                modules_list += 'secFilings'
            if self._include_upgrade_downgrade_history:
                modules_list += 'upgradeDowngradeHistory'
            if self._include_net_share_purchase_activity:
                modules_list += 'netSharePurchaseActivity'

        return {'modules': modules_list}

    @property
    def profile(self):
        """
        Gets the campanies profile:
        :return:
        """
        if self._profile:
            return self._profile
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_asset_profile:
                        return self._profile
                    else:
                        raise ValueError('Asset profile not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def company_officers(self):
        if self._company_officers:
            return self._company_officers
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_asset_profile:
                        return self._company_officers
                    else:
                        raise ValueError('Asset profile not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def income_statement_history(self):
        if self._income_statement_history:
            return self._income_statement_history
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_income_statement_history:
                        return self._income_statement_history
                    else:
                        raise ValueError('Income statement history not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def income_statement_history_quarterly(self):
        if self._income_statement_history_quarterly:
            return self._income_statement_history_quarterly
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_balance_sheet_history_quarterly:
                        return self._income_statement_history_quarterly
                    else:
                        raise ValueError('Quartery income statement history not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def balance_sheet_history(self):
        if self._balance_sheet_history:
            return self._balance_sheet_history
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_balance_sheet_history:
                        return self._balance_sheet_history
                    else:
                        raise ValueError('Balance sheet history not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def balance_sheet_history_quarterly(self):
        if self._balance_sheet_history_quarterly:
            return self._balance_sheet_history_quarterly
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_balance_sheet_history_quarterly:
                        return self._balance_sheet_history_quarterly
                    else:
                        raise ValueError('Quarterly balance sheet history not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def cash_flow_statement_history(self):
        if self._cash_flow_statement_history:
            return self._cash_flow_statement_history
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_cash_flow_statement_history:
                        return self._cash_flow_statement_history
                    else:
                        raise ValueError('Cash flow history not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def cash_flow_statement_history_quarterly(self):
        if self._cash_flow_statement_history_quarterly:
            return self._cash_flow_statement_history_quarterly
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_balance_sheet_history_quarterly:
                        return self._cash_flow_statement_history_quarterly
                    else:
                        raise ValueError('Quarterly cash flow history not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def earnings_estimates(self):
        if self._earnings_estimates:
            return self._earnings_estimates
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_earnings:
                        return self._earnings_estimates
                    else:
                        raise ValueError('Earnings not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def earnings(self):
        if self._earnings_quarterly:
            return self._earnings_quarterly
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_earnings:
                        return self._earnings_quarterly
                    else:
                        raise ValueError('Earnings not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def financials_quarterly(self):
        if self._financials_quarterly:
            return self._financials_quarterly
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_earnings:
                        return self._financials_quarterly
                    else:
                        raise ValueError('Earnings not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def financials_yearly(self):
        if self._financials_yearly:
            return self._financials_yearly
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_earnings:
                        return self._financials_yearly
                    else:
                        raise ValueError('Earnings not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def earnings_history(self):
        if self._earnings_history:
            return self._earnings_history
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_earnings_history:
                        return self._earnings_history
                    else:
                        raise ValueError('Earnings history not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def financial_data(self):
        if self._financial_data:
            return self._financial_data
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_financial_data:
                        return self.financial_data
                    else:
                        raise ValueError('Financial data not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def default_key_statistics(self):
        if self._default_key_statistics:
            return self._default_key_statistics
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_default_key_statistics:
                        return self._default_key_statistics
                    else:
                        raise ValueError('Default key statistics not requested.')
                else:
                    raise ValueError('Financials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def institution_ownership(self):
        if self._institution_ownership:
            return self._institution_ownership
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_institution_ownership:
                        return self._institution_ownership
                    else:
                        raise ValueError('Institutional ownership not requested.')
                else:
                    raise ValueError('Holders not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def insider_holders(self):
        if self._insider_holders:
            return self._insider_holders
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_insider_holders:
                        return self._insider_holders
                    else:
                        raise ValueError('Insider holders not requested.')
                else:
                    raise ValueError('Holders not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def insider_transactions(self):
        if self._insider_transactions:
            return self._insider_transactions
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_insider_transactions:
                        return self._insider_transactions
                    else:
                        raise ValueError('Insider transactions not requested.')
                else:
                    raise ValueError('Holders not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def fund_ownership(self):
        if self._fund_ownership:
            return self._fund_ownership
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_fund_ownership:
                        return self._fund_ownership
                    else:
                        raise ValueError('Fund ownership not requested.')
                else:
                    raise ValueError('Holders not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def major_direct_holders(self):
        if self._major_direct_holders:
            return self._major_direct_holders
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_major_direct_holders:
                        return self._major_direct_holders
                    else:
                        raise ValueError('Major direct holders not requested.')
                else:
                    raise ValueError('Holders not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def major_holders_breakdown(self):
        if self._major_direct_holders_breakdown:
            return self._major_direct_holders_breakdown
        else:
            if self.read_called:
                if self._include_financials:
                    if self._include_major_holders_breakdown:
                        return self._major_direct_holders_breakdown
                    else:
                        raise ValueError('Major direct holders breakdown not requested.')
                else:
                    raise ValueError('Holders not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def recommendation_trend(self):
        if self._recommendation_trend:
            return self._recommendation_trend
        else:
            if self.read_called:
                if self._include_trends:
                    if self._include_recommendation_trend:
                        return self._recommendation_trend
                    else:
                        raise ValueError('Recommendation trend not requested.')
                else:
                    raise ValueError('Trends not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def earnings_trend(self):
        if self._earnings_trend:
            return self._earnings_trend
        else:
            if self.read_called:
                if self._include_trends:
                    if self._include_earnings_trend:
                        return self._earnings_trend
                    else:
                        raise ValueError('Earnings trend not requested.')
                else:
                    raise ValueError('Trends not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def industry_trend(self):
        if self._industry_trend:
            return self._industry_trend
        else:
            if self.read_called:
                if self._include_trends:
                    if self._include_industry_trend:
                        return self._industry_trend
                    else:
                        raise ValueError('Industry trend not requested.')
                else:
                    raise ValueError('Trends not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def index_trend(self):
        if self._index_trend:
            return self._index_trend
        else:
            if self.read_called:
                if self._include_trends:
                    if self._include_index_trend:
                        return self._index_trend
                    else:
                        raise ValueError('Index trend not requested.')
                else:
                    raise ValueError('Trends not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def sector_trend(self):
        if self._sector_trend:
            return self._sector_trend
        else:
            if self.read_called:
                if self._include_trends:
                    if self._include_sector_trend:
                        return self._sector_trend
                    else:
                        raise ValueError('Sector trend not requested.')
                else:
                    raise ValueError('Trends not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def calendar_events_earnings(self):
        if self._calendar_events_earnings:
            return self._calendar_events_earnings
        else:
            if self.read_called:
                if self._include_nonfinancials:
                    if self._include_calendar_events:
                        return self._calendar_events_earnings
                    else:
                        raise ValueError('Calendar events not requested.')
                else:
                    raise ValueError('Nonfinancials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def calendar_events_dividends(self):
        if self._calendar_events_dividends:
            return self._calendar_events_dividends
        else:
            if self.read_called:
                if self._include_nonfinancials:
                    if self._include_calendar_events:
                        return self._calendar_events_dividends
                    else:
                        raise ValueError('Calendar events not requested.')
                else:
                    raise ValueError('Nonfinancials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def sec_filings(self):
        if self._sec_filings:
            return self._sec_filings
        else:
            if self.read_called:
                if self._include_nonfinancials:
                    if self._include_sec_filings:
                        return self._sec_filings
                    else:
                        raise ValueError('SEC filings not requested.')
                else:
                    raise ValueError('Nonfinancials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def upgrade_downgrade_history(self):
        if self._upgrade_downgrade_history:
            return self._upgrade_downgrade_history
        else:
            if self.read_called:
                if self._include_nonfinancials:
                    if self._include_upgrade_downgrade_history:
                        return self._upgrade_downgrade_history
                    else:
                        raise ValueError('Upgrade and downgrade history not requested.')
                else:
                    raise ValueError('Nonfinancials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    @property
    def net_share_purchase_activity(self):
        if self._net_share_purchase_activity:
            return self._net_share_purchase_activity
        else:
            if self.read_called:
                if self._include_nonfinancials:
                    if self._include_net_share_purchase_activity:
                        return self._net_share_purchase_activity
                    else:
                        raise ValueError('Net purchase share activity not requested.')
                else:
                    raise ValueError('Nonfinancials not requested.')
            else:
                raise RuntimeError('Read not called. No data retrieved yet.')

    def _check_data(self, data=None):

        if 'Will be right back' in data.text:
            error = YahooExceptions.YahooRuntimeError('Yahoo Finance is currently down.')

        elif data.json()['quoteSummary']['error']:
            error = YahooExceptions.YahooRequestError(str(data.json()['chart']['error']['description']))

        elif not data.json()['quoteSummary'] or not data['quoteSummary']['result']:
            error = YahooExceptions.YahooRequestError('No data')

        else:
            error = YahooExceptions.YahooError('An error occurred in Yahoo\'s response.')

        return error

    def _parse_data(self, data):

        if self._include_financials:
            self._parse_financials(data)

        if self._include_holders:
            self._parse_holders(data)

        if self._include_trends:
            self._parse_trends(data)

        if self._include_nonfinancials:
            self._parse_nonfinancials(data)

    def _parse_financials(self, data):
        module_dict = data['quoteSummary']['result'][0]

        if self._include_asset_profile:
            self._profile, self._company_officers = self._parse_asset_profile_module(module_dict,
                                                                                     'assetProfile')

        if self._include_income_statement_history:
            self._income_statement_history = self._parse_module(module_dict,
                                                                'incomeStatementHistory',
                                                                'incomeStatementHistory')

        if self._include_income_statement_history_quarterly:
            self._income_statement_history_quarterly = self._parse_module(module_dict,
                                                                          'incomeStatementHistoryQuarterly',
                                                                          'incomeStatementHistory')

        if self._include_balance_sheet_history:
            self._balance_sheet_history = self._parse_module(module_dict,
                                                            'balanceSheetHistory',
                                                            'balanceSheetStatements')

        if self._include_balance_sheet_history_quarterly:
            self._balance_sheet_history_quarterly = self._parse_module(module_dict,
                                                            'balanceSheetHistoryQuarterly',
                                                            'balanceSheetStatements')

        if self._include_cash_flow_statement_history:
            self._cash_flow_statement_history = self._parse_module(module_dict,
                                                                   'cashflowStatementHistory',
                                                                   'cashflowStatements')

        if self._include_cash_flow_statement_history_quarterly:
            self._cash_flow_statement_history = self._parse_module(module_dict,
                                                                   'cashflowStatementHistoryQuarterly',
                                                                   'cashflowStatements')

        if self._include_earnings:
            self._earnings_estimates, self._earnings_quarterly, \
            self._financials_quarterly, self._financials_yearly = self._parse_module(module_dict,
                                                                  'earnings')

        if self._include_earnings_history:
            self._earnings_history = self._parse_module(module_dict,
                                                        'earningsHistory',
                                                        'history')

        if self._include_financial_data:
            self._financial_data = self._parse_module(module_dict, 'financialData')

        if self._include_default_key_statistics:
            self._default_key_statistics = self._parse_module(module_dict,
                                                              'defaultKeyStatistics')

    def _parse_holders(self, data):
        module_dict = data['quoteSummary']['result'][0]

        if self._include_institution_ownership:
            self._institution_ownership = self._parse_module(module_dict,
                                                             'institutionOwnership',
                                                             'ownershipList')

        if self._include_insider_holders:
            self._insider_holders = self._parse_module(module_dict,
                                                       'insiderHolders',
                                                       'holders')

        if self._include_insider_transactions:
            self._insider_transactions = self._parse_module(module_dict,
                                                            'insiderTransactions',
                                                            'transactions')

        if self._include_fund_ownership:
            self._fund_ownership = self._parse_module(module_dict,
                                                      'fundOwnership',
                                                      'ownershipList')

        if self._include_major_direct_holders:
            self._major_direct_holders = self._parse_module(module_dict,
                                                            'majorDirectHolders',
                                                            'holders')

        if self._include_major_holders_breakdown:
            self._major_direct_holders_breakdown = self._parse_module(module_dict,
                                                                      'majorHoldersBreakdown')

    def _parse_trends(self, data):
        module_dict = data['quoteSummary']['result'][0]

        if self._include_recommendation_trend:
            self._recommendation_trend = self._parse_module(module_dict, 'recommendationTrend', 'trend')

        if self._include_earnings_trend:
            self._earnings_trend = self._parse_module(module_dict, 'earningsTrend', 'trend')

        if self._include_industry_trend:
            self._industry_trend = self._parse_module(module_dict, 'industryTrend')

        if self._include_index_trend:
            self._index_trend = self._parse_index_trend_module(module_dict, 'indexTrend')

        if self._include_sector_trend:
            self._sector_trend = self._parse_module(module_dict, 'sectorTrend')

    def _parse_nonfinancials(self, data):
        module_dict = data['quoteSummary']['result'][0]

        if self._include_calendar_events:
            self._calendar_events_earnings, \
            self._calendar_events_dividends = self._parse_calendar_events_module(module_dict, 'calendarEvents')

        if self._include_sec_filings:
            self._sec_filings = self._parse_module(module_dict, 'secFilings', 'filings')

        if self._include_upgrade_downgrade_history:
            self._upgrade_downgrade_history = self._parse_module(module_dict, 'upgradeDowngradeHistory', 'history')

        if self._include_net_share_purchase_activity:
            self._net_share_purchase_activity = self._parse_module(module_dict, 'netSharePurchaseActivity')

    def _parse_module(self, results_dict, module_name, submodule_name=None):
        try:
            if not submodule_name:
                module = results_dict[module_name]
            else:
                module = results_dict[module_name][submodule_name]

                module = self._format_dataframe(module)

        except:
            print('except')

        return module

    def _format_dataframe(self, data_dictionary):
        if isinstance(data_dictionary, list):
            module = [self.flatten(data) for data in data_dictionary]
            module = pd.DataFrame(module)
        else:
            module = self.flatten(data_dictionary)
            module = pd.DataFrame([module])

        module_columns = [column for column in module.columns
                          if not ('.fmt' in column or '.longFmt' in column)]
        module = module[module_columns]

        new_columns_dict = {col: self.pep_pattern.sub('_', col.split('.')[0]).lower() for col in
                            module.columns}
        module.rename(columns=new_columns_dict, inplace=True)

        return module

    def _parse_asset_profile_module(self, results_dict, module_name):
        try:
            profile_dict = results_dict[module_name]
            company_officers_dict = profile_dict['companyOfficers']
            del profile_dict['companyOfficers']

            profile = self._format_dataframe(profile_dict)
            company_officers = self._format_dataframe(company_officers_dict)

        except Exception as e:
            print('except')

        return profile, company_officers

    def _parse_earnings_module(self, results_dict, module_name):
        try:
            module = results_dict[module_name]

            earnings_estimates = module['earningsChart']
            earnings_quarterly = earnings_estimates['quarterly']
            del earnings_estimates['quarterly']

            if isinstance(earnings_estimates['earningsDate'], list):
                earnings_estimates['earningsDate'] = earnings_estimates['earningsDate'][0]

            earnings_estimates = self._format_dataframe(earnings_estimates)
            earnings_quarterly = self._format_dataframe(earnings_quarterly)

            financial_yearly = module['financialData']['yearly']
            financial_quarterly = module['financialData']['quarterly']

            financial_yearly = self._format_dataframe(financial_yearly)
            financial_quarterly = self._format_dataframe(financial_quarterly)

        except:
            print('except')

        return earnings_estimates, earnings_quarterly, financial_quarterly, financial_yearly

    def _parse_index_trend_module(self, results_dict, module_name):
        try:
            index_trend_info = results_dict[module_name]
            index_trend_estimates = index_trend_info['estimates']
            del index_trend_info['estimates']

            index_trend_info = self._format_dataframe(index_trend_info)
            index_trend_estimates = self._format_dataframe(index_trend_estimates)

        except:
            print('except')

        return index_trend_info, index_trend_estimates

    def _parse_calendar_events_module(self, results_dict, module_name):
        try:
            calender_events_earnings = results_dict[module_name]['earnings']

            if isinstance(calender_events_earnings['earningsDate'], list):
                calender_events_earnings['earningsDate'] = calender_events_earnings['earningsDate'][0]

            calender_events_earnings = self._format_dataframe(calender_events_earnings)

            dividends = results_dict[module_name]
            del dividends['earnings']

            calendar_events_dividends = self._format_dataframe(dividends)

        except:
            print('except')

        return calender_events_earnings, calendar_events_dividends

    def flatten(self, d, parent_key='', sep='.'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
