from quantpy.data.base.BaseReader import BaseReader
from quantpy.data.yahoo.YahooSummaryResponse import YahooSummaryResponse
from quantpy.utils.utils import flatten

import quantpy.data.yahoo.YahooExceptions as YahooExceptions
import pandas as pd
import re


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
                 timeout=5.0):
        """
        Constructor for the YahooSummaryReader class to read copmany summaries from the Yahoo Finance API.
        :param symbols: The company(s) for which summaries are to be retrieved.
        :type symbols: Union[str, list]
        :param include_asset_profile: Include a company's profile and company officer list.
        :type include_asset_profile: bool
        :param include_income_statement_history: Include a company's income statement history?
        :type include_income_statement_history: bool
        :param include_income_statement_history_quarterly: Include a company's quarterly income statement history
        :type include_income_statement_history_quarterly: bool
        :param include_balance_sheet_history: Include a company's balance sheet history.
        :type include_balance_sheet_history: bool
        :param include_balance_sheet_history_quarterly: Include a company's quarterly balance sheet history.
        :type include_balance_sheet_history_quarterly: bool
        :param include_cash_flow_statement_history: Include a company's cash flow statement history.
        :type include_cash_flow_statement_history: bool
        :param include_cash_flow_statement_history_quarterly: Include a company's quarterly cash flow statement history.
        :type include_cash_flow_statement_history_quarterly: bool
        :param include_earnings: Include a company's earnings estimates and financials.
        :type include_earnings: bool
        :param include_earnings_history: Include a company's earnings history.
        :type include_earnings_history: bool
        :param include_financial_data: Include a company's financial data.
        :type include_financial_data: bool
        :param include_default_key_statistics: Include a company's key statistics.
        :type include_default_key_statistics: bool
        :param include_institution_ownership: Include a company's insitutional owners.
        :type include_institution_ownership: bool
        :param include_insider_holders: Include a company's insider holders.
        :type include_insider_holders: bool
        :param include_insider_transactions: Include a company's insider transactions.
        :type include_insider_transactions: bool
        :param include_fund_ownership: Include a company's fund owners.
        :type include_fund_ownership: bool
        :param include_major_direct_holders: Include a company's major direct holders.
        :type include_major_direct_holders: bool
        :param include_major_direct_holders_breakdown: Include a company's major direct holders breakdown.
        :type include_major_direct_holders_breakdown: bool
        :param include_recommendation_trend: Include a company's recommendation trend.
        :type include_recommendation_trend: bool
        :param include_earnings_trend: Include a company's earnings trend.
        :type include_earnings_trend: bool
        :param include_industry_trend: Include a company's industry trend.
        :type include_industry_trend: bool
        :param include_index_trend: Include a company's index trend information and estimates.
        :type include_index_trend: bool
        :param include_sector_trend: Include a company's sector trend.
        :type include_sector_trend: bool
        :param include_calendar_events: Include a company's earnings events and dividends events.
        :type include_calendar_events: bool
        :param include_sec_filings: Include a company's SEC filings history.
        :type include_sec_filings: bool
        :param include_upgrade_downgrade_history: Include a company's upgrade and downgrade history.
        :type include_upgrade_downgrade_history: bool
        :param include_net_share_purchase_activity: Include a company's net purchase activity.
        :type include_net_share_purchase_activity: bool
        :param include_all: Include all available information about the company.
        :type include_all: bool
        :param timeout: How long too allow for the request before it
        :type timeout: float
        """
        # Assign all financial information to the value requested.
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

        # Assign all holders information to the value requested.
        self.__include_institution_ownership = include_all or include_institution_ownership
        self.__include_insider_holders = include_all or include_insider_holders
        self.__include_insider_transactions = include_all or include_insider_transactions
        self.__include_fund_ownership = include_all or include_fund_ownership
        self.__include_major_direct_holders = include_all or include_major_direct_holders
        self.__include_major_direct_holders_breakdown = include_all or include_major_direct_holders_breakdown

        # Assign all trends information to the value requested.
        self.__include_recommendation_trend = include_all or include_recommendation_trend
        self.__include_earnings_trend = include_all or include_earnings_trend
        self.__include_industry_trend = include_all or include_industry_trend
        self.__include_index_trend = include_all or include_index_trend
        self.__include_sector_trend = include_all or include_sector_trend

        # Assign all non-financial information to the value requested.
        self.__include_calendar_events = include_all or include_calendar_events
        self.__include_sec_filings = include_all or include_sec_filings
        self.__include_upgrade_downgrade_history = include_all or include_upgrade_downgrade_history
        self.__include_net_share_purchase_activity = include_all or include_net_share_purchase_activity

        self.__include_all = include_all

        # Set the pattern used to define the response dataframe schema formatting.
        self.__pep_pattern = re.compile(r'(?<!^)(?=[A-Z])')

        # Call the super class's constructor.
        super().__init__(symbols=symbols, timeout=timeout)

    @property
    def _url(self):
        """
        Property to get the url for the Yahoo Finance summary API. The API needs to be formatted with a symbol.
        :return: A string containing the API url that needs to be formatted.
        :rtype: str
        """

        return 'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{}'

    @property
    def _params(self):
        """
        Property to get a string containing the comma separated list of parameters for the Yahoo Finance summary API.
        :return: A dict containing a modules key with a string containing the comma separated list of parameters value.
        :rtype: dict
        """

        # Set the string list to empty.
        modules_list = ''

        # Check to see which financial parameters need to be included in the string list.
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

        # Check to see which holders parameters need to be included in the string list.
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

        # Check to see which trends parameters need to be included in the string list.
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

        # Check to see which non-financial parameters need to be included in the string list.
        if self.__include_calendar_events:
            modules_list += 'calendarEvents,'
        if self.__include_sec_filings:
            modules_list += 'secFilings,'
        if self.__include_upgrade_downgrade_history:
            modules_list += 'upgradeDowngradeHistory,'
        if self.__include_net_share_purchase_activity:
            modules_list += 'netSharePurchaseActivity,'

        # Create the dict and return it.
        return {'modules': modules_list}

    def _check_init_args(self):
        """
        Method to make sure that at least one piece of summary data was requested.
        :raise ValueError: If no summary values were requested.
        """

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
            return YahooSummaryResponse(symbol,
                                        exception=YahooExceptions.YahooRuntimeError('Yahoo Finance is currently down.'))
        else:
            data_json = data.json()
            if data_json['quoteSummary']['result'] is None and data_json['quoteSummary']['error'] is not None:
                return YahooSummaryResponse(symbol, exception=data_json['quoteSummary']['error']['description'])
            else:
                return YahooSummaryResponse(symbol, exception=data_json)

    def _parse_response_error(self, symbol, exception):
        """
        Overridden method to handle exception raised from readed the Yahoo Finance API response.
        :param symbol: The symbol of the company for which the data is being parsed.
        :type symbol: str
        :param exception: The exception that occured when reading from the Yahoo Finance API reponse.
        :return: An empty YahooSummaryReponse object containing the exception.
        :rtype: YahooSummaryResponse
        """
        return YahooSummaryResponse(symbol, exception)

    def _parse_response(self, symbol, response_data):
        """
        Overridden method to parse through the data revieved from the API call. It will get the list of modules and then
        it will parse the modules that were requested.
        :param symbol: The symbol of the company for which the data is being parsed.
        :type symbol: str
        :param response_data: The unformatted JSON dictionary data of the company retrieved from the call.
        :type response_data: dict
        :return: An object containing all the data requested and recieved from the Yahoo Finance API call.
        :rtype: YahooSummaryResponse
        """

        # Get the modules dictionary from the unformatted JSON dictionary. Note, if the modules dictionary is not found,
        # this will raise an error.
        try:
            modules = response_data['quoteSummary']['result'][0]
        except Exception as e:
            return YahooSummaryResponse(symbol, str(YahooExceptions.YahooAllModulesNotFoundError(e)))

        # Instantiate the YahooSummaryResponse object.
        ys = YahooSummaryResponse(symbol)

        if self.__include_asset_profile:
            ys.profile = self._parse_module_asset_profile_profile(modules, 'assetProfile')
            ys.company_officers = self._parse_module_asset_profile_company_officers(modules, 'assetProfile')

        if self.__include_income_statement_history:
            ys.income_statement_history = self._parse_module(modules, 'incomeStatementHistory',
                                                             'incomeStatementHistory')

        if self.__include_income_statement_history_quarterly:
            ys.income_statement_history_quarterly = self._parse_module(modules, 'incomeStatementHistoryQuarterly',
                                                                       'incomeStatementHistory')

        if self.__include_balance_sheet_history:
            ys.balance_sheet_history = self._parse_module(modules, 'balanceSheetHistory', 'balanceSheetStatements')

        if self.__include_balance_sheet_history_quarterly:
            ys.balance_sheet_history_quarterly = self._parse_module(modules, 'balanceSheetHistoryQuarterly',
                                                                    'balanceSheetStatements')

        if self.__include_cash_flow_statement_history:
            ys.cash_flow_statement_history = self._parse_module(modules, 'cashflowStatementHistory',
                                                                'cashflowStatements')

        if self.__include_cash_flow_statement_history_quarterly:
            ys.cash_flow_statement_history_quarterly = self._parse_module(modules, 'cashflowStatementHistoryQuarterly',
                                                                          'cashflowStatements')

        if self.__include_earnings:
            ys.earnings_estimates = self._parse_module_earnings_estimates(modules, 'earnings')
            ys.earnings_estimates_quarterly = self._parse_module_earnings_estimates_quarterly(modules, 'earnings')
            ys.financials_yearly = self._parse_module_earnings_finance_yearly(modules, 'earnings')
            ys.financials_quarterly = self._parse_module_earnings_finance_quarterly(modules, 'earnings')

        if self.__include_earnings_history:
            ys.earnings_history = self._parse_module(modules, 'earningsHistory', 'history')

        if self.__include_financial_data:
            ys.financial_data = self._parse_module(modules, 'financialData')

        if self.__include_default_key_statistics:
            ys.default_key_statistics = self._parse_module(modules, 'defaultKeyStatistics')

        if self.__include_institution_ownership:
            ys.institution_ownership = self._parse_module(modules, 'institutionOwnership', 'ownershipList')

        if self.__include_insider_holders:
            ys.insider_holders = self._parse_module(modules, 'insiderHolders', 'holders')

        if self.__include_insider_transactions:
            ys.insider_transactions = self._parse_module(modules, 'insiderTransactions', 'transactions')

        if self.__include_fund_ownership:
            ys.fund_ownership = self._parse_module(modules, 'fundOwnership', 'ownershipList')

        if self.__include_major_direct_holders:
            ys.major_direct_holders = self._parse_module(modules, 'majorDirectHolders', 'holders')

        if self.__include_major_direct_holders_breakdown:
            ys.major_direct_holders_breakdown = self._parse_module(modules, 'majorHoldersBreakdown')

        if self.__include_recommendation_trend:
            ys.recommendation_trend = self._parse_module(modules, 'recommendationTrend', 'trend')

        if self.__include_earnings_trend:
            ys.earnings_trend = self._parse_module(modules, 'earningsTrend', 'trend')

        if self.__include_industry_trend:
            ys.industry_trend = self._parse_module(modules, 'industryTrend')

        if self.__include_index_trend:
            ys.index_trend_info = self._parse_module_index_trend_info(modules, 'indexTrend')
            ys.index_trend_estimate = self._parse_module_index_trend_estimates(modules, 'indexTrend')

        if self.__include_sector_trend:
            ys.sector_trend = self._parse_module(modules, 'sectorTrend')

        if self.__include_calendar_events:
            ys.calendar_events_earnings = self._parse_module_calendar_events_earnings(modules, 'calendarEvents')
            ys.calendar_events_dividends = self._parse_module_calendar_events_dividends(modules, 'calendarEvents')

        if self.__include_sec_filings:
            ys.sec_filings = self._parse_module(modules, 'secFilings', 'filings')

        if self.__include_upgrade_downgrade_history:
            ys.upgrade_downgrade_history = self._parse_module(modules, 'upgradeDowngradeHistory', 'history')

        if self.__include_net_share_purchase_activity:
            ys.net_share_purchase_activity = self._parse_module(modules, 'netSharePurchaseActivity')

        return ys

    def _parse_module(self, modules_dict, module_name, submodule_name=None):
        """
        Method to parse an individual requested module returned from the Yahoo Finance API call.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :param submodule_name: The name of the submodule to be parsed. Some modules are wrapped in another dictionary.
        :type submodule_name: str
        :return: A tuple containing a dataframe containing the information and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the module to be parsed. If the module is not found, then it will return None and a
        # YahooModuleNotFoundError.
        try:
            # Checks to see if there is a submodule.
            if not submodule_name:
                module = modules_dict[module_name]
            else:
                module = modules_dict[module_name][submodule_name]

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            module_dataframe = self._format_dataframe(module)

        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the module information and
        # None since there was no error.
        return module_dataframe, None

    def _parse_module_asset_profile_profile(self, modules_dict, module_name):
        """
        Method to parse the assetProfile module returned from the Yahoo Finance API call. The assetProfile module is
        uniquely parsed because it contains two separate pieces of data: The company profile and the company officers.
        This method will parse the profile portion of the assetProfile.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :return: A tuple containing a dataframe containing the company profile and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the assetProfile module. If the module is not found, then it will return None and a
        # YahooModuleNotFoundError.
        try:
            profile_dict = modules_dict[module_name]

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            # We are going to remove the company officer's submodule to be parsed later.
            del profile_dict['companyOfficers']
            profile = self._format_dataframe(profile_dict)

        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the profile
        # information and None since there was no error.
        return profile, None

    def _parse_module_asset_profile_company_officers(self, modules_dict, module_name):
        """
        Method to parse the assetProfile module returned from the Yahoo Finance API call. The assetProfile module is
        uniquely parsed because it contains two separate pieces of data: The company profile and the company officers.
        This method will parse the company officers portion of the assetProfile.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :return: A tuple containing a dataframe containing the company profile and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the assetProfile and the company_officers submodule. If the module is not found, then it will
        # return None and a YahooModuleNotFoundError.
        try:
            profile_dict = modules_dict[module_name]
            company_officers_dict = profile_dict['companyOfficers']

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            company_officers = self._format_dataframe(company_officers_dict)

        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the company officers
        # information and None since there was no error.
        return company_officers, None

    def _parse_module_earnings_estimates(self, modules_dict, module_name):
        """
        Method to parse the earnings module returned from the Yahoo Finance API call. The earnings module is uniquely
        parsed because it contains four separate pieces of data: The company earnings estimates, the company quarterly
        earnings estimates, the company's yearly financials, and the company's quarterly financials.
        This method will parse the company's earnings estimates.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :return: A tuple containing a dataframe containing the company profile and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the earnings and the earningsChart submodule. If the module is not found, then it will
        # return None and a YahooModuleNotFoundError.
        try:
            earnings_estimates_dict = modules_dict[module_name]['earningsChart']
            del earnings_estimates_dict['quarterly']

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            # Sometimes the earnings data portion is returned as a list instead of a date.
            if isinstance(earnings_estimates_dict['earningsDate'], list):
                earnings_estimates_dict['earningsDate'] = earnings_estimates_dict['earningsDate'][0]

            earnings_estimates = self._format_dataframe(earnings_estimates_dict)

        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the company officers
        # information and None since there was no error.
        return earnings_estimates, None

    def _parse_module_earnings_estimates_quarterly(self, modules_dict, module_name):
        """
        Method to parse the earnings module returned from the Yahoo Finance API call. The earnings module is uniquely
        parsed because it contains four separate pieces of data: The company earnings estimates, the company quarterly
        earnings estimates, the company's yearly financials, and the company's quarterly financials.
        This method will parse the company's quarterly earnings estimates.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :return: A tuple containing a dataframe containing the company profile and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the earnings, earningsChart, and quarterly submodule. If the module is not found, then it will
        # return None and a YahooModuleNotFoundError.
        try:
            module = modules_dict[module_name]
            quarterly_earnings_estimates = module['earningsChart']['quarterly']

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            earnings_quarterly = self._format_dataframe(quarterly_earnings_estimates)

        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the company officers
        # information and None since there was no error.
        return earnings_quarterly, None

    def _parse_module_earnings_finance_yearly(self, modules_dict, module_name):
        """
        Method to parse the earnings module returned from the Yahoo Finance API call. The earnings module is uniquely
        parsed because it contains four separate pieces of data: The company earnings estimates, the company quarterly
        earnings estimates, the company's yearly financials, and the company's quarterly financials.
        This method will parse the company's yearly financials.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :return: A tuple containing a dataframe containing the company profile and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the earnings, financialsChart, and yearly submodule. If the module is not found, then it will
        # return None and a YahooModuleNotFoundError.
        try:
            module = modules_dict[module_name]
            financial_yearly = module['financialsChart']['yearly']

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            financial_yearly = self._format_dataframe(financial_yearly)

        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the company officers
        # information and None since there was no error.
        return financial_yearly, None

    def _parse_module_earnings_finance_quarterly(self, modules_dict, module_name):
        """
        Method to parse the earnings module returned from the Yahoo Finance API call. The earnings module is uniquely
        parsed because it contains four separate pieces of data: The company earnings estimates, the company quarterly
        earnings estimates, the company's yearly financials, and the company's quarterly financials.
        This method will parse the company's quarterly financials.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :return: A tuple containing a dataframe containing the company profile and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the earnings, financialsChart, and quarterly submodule. If the module is not found, then it
        # will return None and a YahooModuleNotFoundError.
        try:
            module = modules_dict[module_name]
            financial_quarterly = module['financialsChart']['quarterly']

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            financial_quarterly = self._format_dataframe(financial_quarterly)

        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the company officers
        # information and None since there was no error.
        return financial_quarterly, None

    def _parse_module_index_trend_info(self, modules_dict, module_name):
        """
        Method to parse the indexTrend module returned from the Yahoo Finance API call. The indexTrend module is
        uniquely parsed because it contains two separate pieces of data: The index trend's information and the index
        trend's estimates. This method will parse the index trend's information portion of the indexTrend.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :return: A tuple containing a dataframe containing the company profile and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the indexTrend module. If the module is not found, then it will return None and a
        # YahooModuleNotFoundError.
        try:
            index_trend_info = modules_dict[module_name]

            # Removes the estimates submodule.
            del index_trend_info['estimates']

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            index_trend_info = self._format_dataframe(index_trend_info)

        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the company officers
        # information and None since there was no error.
        return index_trend_info, None

    def _parse_module_index_trend_estimates(self, modules_dict, module_name):
        """
        Method to parse the indexTrend module returned from the Yahoo Finance API call. The indexTrend module is
        uniquely parsed because it contains two separate pieces of data: The index trend's information and the index
        trend's estimates. This method will parse the index trend's estimates portion of the indexTrend.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :return: A tuple containing a dataframe containing the company profile and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the indexTrend module and the estimates submodule. If the module is not found, then it will
        # return None and a YahooModuleNotFoundError.
        try:
            index_trend_info = modules_dict[module_name]
            index_trend_estimates = index_trend_info['estimates']

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            index_trend_estimates = self._format_dataframe(index_trend_estimates)

        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the company officers
        # information and None since there was no error.
        return index_trend_estimates, None

    def _parse_module_calendar_events_earnings(self, modules_dict, module_name):
        """
        Method to parse the calendarEvents module returned from the Yahoo Finance API call. The calendarEvents module is
        uniquely parsed because it contains two separate pieces of data: The company's earnings events and the company's
        dividends dates. This method will parse the company's earnings dates portion of the calendarEvents.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :return: A tuple containing a dataframe containing the company profile and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the calendarEvents module and the earnings submodule. If the module is not found, then it will
        # return None and a YahooModuleNotFoundError.
        try:
            calender_events_earnings = modules_dict[module_name]['earnings']

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            if isinstance(calender_events_earnings['earningsDate'], list):
                calender_events_earnings['earningsDate'] = calender_events_earnings['earningsDate'][0]

            calender_events_earnings = self._format_dataframe(calender_events_earnings)
        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the company officers
        # information and None since there was no error.
        return calender_events_earnings, None

    def _parse_module_calendar_events_dividends(self, modules_dict, module_name):
        """
        Method to parse the calendarEvents module returned from the Yahoo Finance API call. The calendarEvents module is
        uniquely parsed because it contains two separate pieces of data: The company's earnings events and the company's
        dividends dates. This method will parse the company's dividends dates portion of the calendarEvents.
        :param modules_dict: A dictionary containing all the modules.
        :type modules_dict: dict
        :param module_name: The name of the module to be parsed.
        :type module_name: str
        :return: A tuple containing a dataframe containing the company profile and None, or None and an error.
        :rtype: tuple
        """

        # Attempt to find the calendarEvents module and the dividends submodule. If the module is not found, then it
        # will return None and a YahooModuleNotFoundError.
        try:
            dividends = modules_dict[module_name]

            # Removes the earnings submodule.
            del dividends['earnings']

        except Exception as e:
            return None, YahooExceptions.YahooModuleNotFoundError(e)

        # Attempt to get a formatted dataframe from the module. If there was an error formatting the dataframe, then it
        # will return None and a YahooModuleFormatError.
        try:
            calendar_events_dividends = self._format_dataframe(dividends)

        except Exception as e:
            return None, YahooExceptions.YahooModuleFormatError(e)

        # If the module was found and formatted, then it will return a dataframe containing the company officers
        # information and None since there was no error.
        return calendar_events_dividends, None

    def _format_dataframe(self, data_dictionary):
        """
        Method to format the response dictionary from a requested module.
        :param data_dictionary: The dictionary data from the requested module.
        :type data_dictionary: dict
        :return: A formatted dataframe containing the data.
        :rtype pd.Dataframe
        """

        # Checks to see if there are any dictionaries or lists within the data that need to be flattened.
        if isinstance(data_dictionary, list):
            module = [flatten(data) for data in data_dictionary]
            module = pd.DataFrame(module)
        else:
            module = flatten(data_dictionary)
            module = pd.DataFrame([module])

        # Due to the way Yahoo Finance API returns numeric types, the raw integer value is preferred. Therefore, any
        # values with the suffix longfmt (long format) or fmt (format) are removed.
        module_columns = [column for column in module.columns
                          if not ('.fmt' in column or '.longFmt' in column)]

        # Get a new dataframe.
        module = module[module_columns]

        # Format the headers of the column to match PEP8 standards.
        new_columns_dict = {col: self.__pep_pattern.sub('_', col.split('.')[0]).lower() for col in
                            module.columns}
        module.rename(columns=new_columns_dict, inplace=True)

        return module
