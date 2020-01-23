from quantpy.data.base.BaseReader import BaseReader
import quantpy.data.yahoo.YahooExceptions as YahooExceptions

from pandas.io.json import json_normalize
import pandas as pd


class YahooTrendReader(BaseReader):

    def __init__(self, symbols, recommendation_trend=True, earnings_trend=True,
                 industry_trend=True, index_trend=True, sector_trend=True,
                 retry_count=3, pause=0.1, timeout=5):
        """
        Initializer function for the YahooHoldersReader class.
        :param symbols: The list of symbols to be used.
        :type symbols: str or list
        :param institutions: Include orgs that trade in large enough quantities
        that it qualifies preferential treatment and lower commissions invested
        in the security.
        :type institutions bool
        :param funds: Include the top mutual funds invested in the security.
        :type funds: bool
        :param insiders: Include directors, senior officers, or any person
        that beneficially owns more than 10% of a company's voting shares
        :type insiders: bool
        :param majors_breakdown: Include the % breakdown of owners.
        :type majors_breakdown: bool
        :param insider_transactions: Include insiders's transactions
        :param retry_count: Optional. The amount of times to retry an api call.
        :type retry_count int
        :param pause: Optional. The amount of time to pause between retries.
        :type pause float
        :param timeout: Optional. The amount of time until a request times out.
        :type timeout int
        """

        self.institutions = institutions
        self.insiders = insiders
        self.funds = funds

        self.majors_breakdown = majors_breakdown
        self.insider_transactions = insider_transactions

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

        if self.institutions:
            modules_list += 'institutionOwnership,'
        if self.funds:
            modules_list += 'fundOwnership,'
        if self.insiders:
            modules_list += 'insiderHolders,'
        if self.majors_breakdown:
            modules_list += 'majorHoldersBreakdown,'
        if self.insider_transactions:
            modules_list += 'insiderTransactions,'

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

    def _organize_data(self, data):
        if self.institutions:
            institutions_dataframe = self._organize_holders(data, 'institutionOwnership')

        if self.funds:
            funds_dataframe = self._organize_holders(data, 'fundOwnership')

        if self.insiders:
            insiders_dataframe = self._organize_insiders(data)

        if self.majors_breakdown:
            majors_breakdown_dataframe = self._organize_breakdowns(data)

        if self.insider_transactions:
            insider_transactions_dataframe = self._organize_insider_transactions(data)

    def _organize_holders(self, data, holder_type):
        institutions_data = data['quoteSummary']['result'][0][holder_type]['ownershipList']

        institutions_data = json_normalize(institutions_data)

        institutions_data = institutions_data[['reportDate.raw', 'pctHeld.raw',
                                              'position.raw', 'value.raw']]

        institutions_data.rename(columns={'reportDate.raw': 'report_date',
                                          'pctHeld.raw': 'percent_held',
                                          'position.raw': 'position',
                                          'value.raw': 'value'}, inplace=True)

        institutions_data['report_date'] = pd.to_datetime(institutions_data['report_date'],
                                                   unit='s')

        return institutions_data

    def _organize_insiders(self, data):
        insiders_data = data['quoteSummary']['result'][0]['insiderHolders']['holders']

        insiders_data = json_normalize(insiders_data)

        insiders_data = insiders_data[['name', 'relation', 'url',
                                      'transactionDescription',
                                      'latestTransDate.raw',
                                      'positionDirect.raw',
                                      'positionDirectDate.raw']]

        insiders_data.rename(columns={'transactionDescription': 'transaction_description',
                                      'latestTransDate.raw': 'latest_transtion_date',
                                      'positionDirect.raw': 'position_direct',
                                      'positionDirectDate.raw': 'position_direct_date'}, inplace=True)

        insiders_data['latest_transtion_date'] = pd.to_datetime(insiders_data['latest_transtion_date'],
                                                                unit='s')

        insiders_data['position_direct_date'] = pd.to_datetime(insiders_data['position_direct_date'],
                                                               unit='s')

        return insiders_data

    def _organize_insider_transactions(self, data):
        insider_transactions_data = data['quoteSummary']['result'][0]['insiderTransactions']['transactions']

        insider_transactions_data = json_normalize(insider_transactions_data)

        insider_transactions_data = insider_transactions_data[['shares.raw', 'value.raw', 'filerUrl',
                                                              'transactionText',
                                                              'filerName',
                                                              'filerRelation',
                                                              'moneyText',
                                                              'startDate']]

        insider_transactions_data.rename(columns={'shares.raw': 'shares',
                                                  'value.raw': 'value',
                                                  'filerUrl': 'filer_url',
                                                  'transactionText': 'transaction_text',
                                                  'filerName': 'filer_name',
                                                  'filerRelation': 'filer_relation',
                                                  'moneyText': 'money_text',
                                                  'startDate': 'start_date'}, inplace=True)

        insider_transactions_data['start_date'] = pd.to_datetime(insider_transactions_data['start_date'],
                                                                unit='s')

        return insider_transactions_data

    def _organize_breakdowns(self, data):
        breakdowns_data = data['quoteSummary']['result'][0]['majorHoldersBreakdown']

        breakdowns_data = json_normalize(breakdowns_data)

        breakdowns_data = breakdowns_data[['insidersPercentHeld.raw', 'institutionsPercentHeld.raw',
                                          'institutionsFloatPercentHeld.raw', 'institutionsCount.raw']]

        breakdowns_data.rename(columns={'insidersPercentHeld.raw': 'insiders_percent_held',
                                        'institutionsPercentHeld.raw': 'institutions_percent_held',
                                        'institutionsFloatPercentHeld.raw': 'institutions_float_percent_held',
                                        'institutionsCount.raw': 'institutions_count'}, inplace=True)

        return breakdowns_data