from quantpy.data.base.BaseReader import BaseReader
import pandas as pd
import collections

import quantpy.data.yahoo.YahooExceptions as YahooExceptions
from quantpy.data.yahoo.YahooSummary import YahooSummary
import re


class YahooSummaryReader(BaseReader):

    def __init__(self, symbols,
                 include_all=True,
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
                 include_major_holders_breakdown=False,
                 include_recommendation_trend=False,
                 include_earnings_trend=False,
                 include_industry_trend=False,
                 include_index_trend=False,
                 include_sector_trend=False,
                 include_calendar_events=False,
                 include_sec_filings=False,
                 include_upgrade_downgrade_history=False,
                 include_net_share_purchase_activity=False,
                 retry_count=3, pause=0.1, timeout=5):

        self._include_asset_profile = include_all or include_asset_profile
        self._include_income_statement_history = include_all or include_income_statement_history
        self._include_income_statement_history_quarterly = include_all or include_income_statement_history_quarterly
        self._include_balance_sheet_history = include_all or include_balance_sheet_history
        self._include_balance_sheet_history_quarterly = include_all or include_balance_sheet_history_quarterly
        self._include_cash_flow_statement_history = include_all or include_cash_flow_statement_history
        self._include_cash_flow_statement_history_quarterly = include_all or include_cash_flow_statement_history_quarterly
        self._include_earnings = include_earnings
        self._include_earnings_history = include_all or include_earnings_history
        self._include_financial_data = include_all or include_financial_data
        self._include_default_key_statistics = include_all or include_default_key_statistics

        self._include_institution_ownership = include_all or include_institution_ownership
        self._include_insider_holders = include_all or include_insider_holders
        self._include_insider_transactions = include_all or include_insider_transactions
        self._include_fund_ownership = include_all or include_fund_ownership
        self._include_major_direct_holders = include_all or include_major_direct_holders
        self._include_major_direct_holders_breakdown = include_all or include_major_holders_breakdown

        self._include_recommendation_trend = include_all or include_recommendation_trend
        self._include_earnings_trend = include_all or include_earnings_trend
        self._include_industry_trend = include_all or include_industry_trend
        self._include_index_trend = include_all or include_index_trend
        self._include_sector_trend = include_all or include_sector_trend

        self._include_calendar_events = include_all or include_calendar_events
        self._include_sec_filings = include_all or include_sec_filings
        self._include_upgrade_downgrade_history = include_all or include_upgrade_downgrade_history
        self._include_net_share_purchase_activity = include_all or include_net_share_purchase_activity

        self._include_all = include_all

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
        if self._include_major_direct_holders_breakdown:
            modules_list += 'majorHoldersBreakdown,'

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

        if self._include_calendar_events:
            modules_list += 'calendarEvents'
        if self._include_sec_filings:
            modules_list += 'secFilings'
        if self._include_upgrade_downgrade_history:
            modules_list += 'upgradeDowngradeHistory'
        if self._include_net_share_purchase_activity:
            modules_list += 'netSharePurchaseActivity'

        return {'modules': modules_list}

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

    def _parse_data(self, data, symbol):
        modules = data['quoteSummary']['result'][0]
        ys = YahooSummary(symbol)

        if self._include_asset_profile:
            try:
                ys.profile, ys.company_officers = self._parse_asset_profile_module(modules, 'assetProfile')
            except Exception as e:
                ys.profile = None, e
                ys.company_officers = None, e

        if self._include_income_statement_history:
            try:
                ys.income_statement_history = self._parse_module(modules, 'incomeStatementHistory', 'incomeStatementHistory')
            except Exception as e:
                ys.income_statement_history = None, e

        if self._include_income_statement_history_quarterly:
            ys.income_statement_history_quarterly = self._parse_module(modules, 'incomeStatementHistoryQuarterly', 'incomeStatementHistory')

        if self._include_balance_sheet_history:
            ys.balance_sheet_history = self._parse_module(modules, 'balanceSheetHistory', 'balanceSheetStatements')

        if self._include_balance_sheet_history_quarterly:
            ys.balance_sheet_history_quarterly = self._parse_module(modules, 'balanceSheetHistoryQuarterly', 'balanceSheetStatements')

        if self._include_cash_flow_statement_history:
            ys.cash_flow_statement_history = self._parse_module(modules, 'cashflowStatementHistory', 'cashflowStatements')

        if self._include_cash_flow_statement_history_quarterly:
            ys.cash_flow_statement_history = self._parse_module(modules, 'cashflowStatementHistoryQuarterly', 'cashflowStatements')

        if self._include_earnings:
            ys.earnings_estimates, ys.earnings_quarterly, ys.financials_quarterly, ys.financials_yearly = self._parse_module(modules, 'earnings')

        if self._include_earnings_history:
            ys.earnings_history = self._parse_module(modules, 'earningsHistory', 'history')

        if self._include_financial_data:
            ys.financial_data = self._parse_module(modules, 'financialData')

        if self._include_default_key_statistics:
            ys.default_key_statistics = self._parse_module(modules, 'defaultKeyStatistics')

        if self._include_institution_ownership:
            ys.institution_ownership = self._parse_module(modules, 'institutionOwnership', 'ownershipList')

        if self._include_insider_holders:
            ys.insider_holders = self._parse_module(modules, 'insiderHolders', 'holders')

        if self._include_insider_transactions:
            ys.insider_transactions = self._parse_module(modules, 'insiderTransactions', 'transactions')

        if self._include_fund_ownership:
            ys.fund_ownership = self._parse_module(modules, 'fundOwnership', 'ownershipList')

        if self._include_major_direct_holders:
            ys.major_direct_holders = self._parse_module(modules, 'majorDirectHolders', 'holders')

        if self._include_major_direct_holders_breakdown:
            ys.major_direct_holders_breakdown = self._parse_module(modules, 'majorHoldersBreakdown')

        if self._include_recommendation_trend:
            ys.recommendation_trend = self._parse_module(modules, 'recommendationTrend', 'trend')

        if self._include_earnings_trend:
            ys.earnings_trend = self._parse_module(modules, 'earningsTrend', 'trend')

        if self._include_industry_trend:
            ys.industry_trend = self._parse_module(modules, 'industryTrend')

        if self._include_index_trend:
            ys.index_trend = self._parse_index_trend_module(modules, 'indexTrend')

        if self._include_sector_trend:
            ys.sector_trend = self._parse_module(modules, 'sectorTrend')

        if self._include_calendar_events:
            ys.calendar_events_earnings, ys.calendar_events_dividends = self._parse_calendar_events_module(modules, 'calendarEvents')

        if self._include_sec_filings:
            ys.sec_filings = self._parse_module(modules, 'secFilings', 'filings')

        if self._include_upgrade_downgrade_history:
            ys.upgrade_downgrade_history = self._parse_module(modules, 'upgradeDowngradeHistory', 'history')

        if self._include_net_share_purchase_activity:
            ys.net_share_purchase_activity = self._parse_module(modules, 'netSharePurchaseActivity')

        return ys

    def _parse_module(self, modules_dict, module_name, submodule_name=None):
        if not submodule_name:
            module = modules_dict[module_name]
        else:
            module = modules_dict[module_name][submodule_name]

        module_dataframe = self._format_dataframe(module)

        return module_dataframe

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
