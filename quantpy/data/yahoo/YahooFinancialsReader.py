from quantpy.data.base.BaseReader import BaseReader
import quantpy.data.yahoo.YahooExceptions as YahooExceptions

from pandas.io.json import json_normalize
import pandas as pd
import re
import numpy as np


class YahooFinancialsReader(BaseReader):

    def __init__(self, symbols, assets=True, income_statement=True, balance_sheet=True,
                 cash_flow_statement=True, earnings=True, financials=True,
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

        self.assets = assets
        self.income_statement = income_statement
        self.balance_sheet = balance_sheet
        self.cash_flow = cash_flow_statement
        self.earnings = earnings
        self.financials = financials

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

        if self.assets:
            modules_list += 'assetProfile,'
        if self.income_statement:
            modules_list += 'incomeStatementHistory,incomeStatementHistoryQuarterly,'
        if self.balance_sheet:
            modules_list += 'balanceSheetHistory,balanceSheetHistoryQuarterly,'
        if self.cash_flow:
            modules_list += 'cashFlowStatementHistory,cashFlowStatementHistoryQuarterly,'
        if self.earnings:
            modules_list += 'earnings,earningsHistory,'
        if self.financials:
            modules_list += 'financialData,'

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
        if self.assets:
            assets_dataframe = self._organize_assets(data)

        if self.income_statement:
            income_statement_dataframe = self._organize_income_statement(data)

        if self.balance_sheet:
            balance_sheet_dataframe = self._organize_balance_sheet(data)

        if self.cash_flow:
            cash_flow_dataframe = self._organize_cash_flow(data)

        if self.earnings:
            earnings_dataframe = self._organize_earnings(data)

        if self.financials:
            financials_dataframe = self._organize_financials(data)

    def _organize_assets(self, data):
        employee_data = data['quoteSummary']['result'][0]['assetProfile']['companyOfficers']

        employee_data = json_normalize(employee_data)

        employee_data = employee_data[['name', 'age', 'title', 'yearBorn',
                                           'fiscalYear', 'totalPay.raw', 'exercisedValue.raw',
                                           'unexercisedValue.raw']]

        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        new_columns_dict = {col: pattern.sub('_', col.split('.')[0]).lower()
                            for col in employee_data.columns}

        employee_data.rename(columns=new_columns_dict, inplace=True)

        employee_data['year_born'] = pd.to_datetime(employee_data['year_born'],
                                                          unit='s')

        return employee_data

    def _organize_income_statement(self, data):
        income_data = data['quoteSummary']['result'][0]['incomeStatementHistoryQuarterly']['incomeStatementHistory']
        income_data += data['quoteSummary']['result'][0]['incomeStatementHistory']['incomeStatementHistory']

        income_data = json_normalize(income_data)


        columns = ['endDate.raw', 'totalRevenue.raw', 'costOfRevenue.raw', 'grossProfit.raw',
                                   'researchDevelopment.raw', 'sellingGeneralAdministrative.raw', 'nonRecurring.raw',
                                   'otherOperatingExpenses.raw', 'totalOperatingExpenses.raw', 'operatingIncome.raw',
                                   'totalOtherIncomeExpenseNet.raw', 'ebit.raw', 'interestExpense.raw', 'incomeBeforeTax.raw',
                                   'incomeTaxExpense.raw', 'minorityInterest.raw', 'netIncomeFromContinuingOps.raw',
                                   'discontinuedOperations.raw', 'extraordinaryItems.raw', 'effectOfAccountingCharges.raw',
                                   'otherItemsr.raw', 'netIncome.raw', 'netIncomeApplicableToCommonShares.raw']

        ins = [col for col in columns if col in income_data.columns.to_list()]
        not_ins = [col for col in columns if col not in income_data.columns.to_list()]

        income_data = income_data[ins]

        for not_in in not_ins:
            income_data[not_in] = np.nan

        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        new_columns_dict = {col: pattern.sub('_', col.split('.')[0]).lower() for col in income_data.columns}

        income_data.rename(columns=new_columns_dict, inplace=True)

        income_data['end_date'] = pd.to_datetime(income_data['end_date'],  unit='s')

        return income_data

    def _organize_balance_sheet(self, data):
        income_data = data['quoteSummary']['result'][0]['cashflowStatementHistory']['cashflowStatements']
        income_data += data['quoteSummary']['result'][0]['cashflowStatementHistoryQuarterly']['cashflowStatements']


        income_data = json_normalize(income_data)

        columns = ['endDate.raw', 'netIncome.raw', 'depreciation.raw', 'changeToNetincome.raw',
                   'changeToAccountReceivables.raw', 'changeToLiabilities.raw', 'changeToInventory.raw',
                   'totalCashFromOperatingActivities.raw', 'capitalExpenditures.raw', 'investments.raw',
                   'otherCashflowsFromInvestingActivities.raw', 'totalCashflowsFromInvestingActivities.raw', 'effectOfExchangeRate.raw',
                   'changeInCash.raw', 'repurchaseOfStock.raw']

        not_ins = [col for col in columns if col not in income_data.columns.to_list()]

        for not_in in not_ins:
            income_data[not_in] = np.nan

        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        new_columns_dict = {col: pattern.sub('_', col.split('.')[0]).lower() for col in
                            income_data.columns}

        income_data.rename(columns=new_columns_dict, inplace=True)

        income_data['end_date'] = pd.to_datetime(income_data['end_date'], unit='s')

        return income_data

    def _organize_cash_flow(self, data):
        cash_flow_data = data['quoteSummary']['result'][0]['incomeStatementHistoryQuarterly'][
            'incomeStatementHistory']

        cash_flow_data = json_normalize(cash_flow_data)

        columns = ['endDate.raw', 'cash.raw', 'shortTermInvestments.raw', 'netReceivables.raw',
                   'inventory.raw', 'otherCurrentAssets.raw', 'totalCurrentAssets.raw',
                   'longTermInvestments.raw', 'propertyPlantEquipment.raw', 'otherAssets.raw',
                   'deferredLongTermAssetCharges.raw', 'totalAssets.raw', 'accountsPayable.raw',
                   'minorityInterest.raw', 'otherCurrentLiab.raw', 'longTermDebt.raw',
                   'otherLiab.raw',
                   'totalCurrentLiabilities.raw', 'totalLiab.raw', 'commonStock.raw',
                   'retainedEarnings.raw', 'treasuryStock.raw', 'capitalSurplus.raw',
                   'otherStockholderEquity.raw', 'totalStockholderEquity.raw',
                   'netTangibleAssets.raw']

        not_ins = [col for col in columns if col not in cash_flow_data.columns.to_list()]

        for not_in in not_ins:
            cash_flow_data[not_in] = np.nan

        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        new_columns_dict = {col: pattern.sub('_', col.split('.')[0]).lower() for col in
                            cash_flow_data.columns}

        cash_flow_data.rename(columns=new_columns_dict, inplace=True)

        cash_flow_data['end_date'] = pd.to_datetime(cash_flow_data['end_date'], unit='s')

        return cash_flow_data

    def _organize_earnings(self, data):
        earnings_data = data['quoteSummary']['result'][0]['earnings']['earningschart']
        earnings_data_quart = data['quoteSummary']['result'][0]['earnings']['earningschart']['quarterly']

        finance_data = data['quoteSummary']['result'][0]['earnings']['financialsChart']['yearly']
        finance_data += data['quoteSummary']['result'][0]['earnings']['financialsChart']['quarterly']


        earnings_data = json_normalize(earnings_data)
        earnings_data_quart = json_normalize(earnings_data_quart.pop('quarterly', None))

        finance_data = json_normalize(finance_data)

        earnings_data_columns = ['date.raw', 'actual.raw', 'estimate.raw']
        earnings_data_quart_columns = ['currentQuarterEstimate', 'currentQuarterEstimateDate',
                                       'currentQuarterEstimateYear', 'earningsDate.raw']
        finance_data_columns = ['date', 'revenue.raw', 'earnings.raw']



        not_in_earnings_data = [col for col in earnings_data_columns if col not in earnings_data.columns.to_list()]
        not_in_earnings_data_quart_columns = [col for col in earnings_data_quart_columns if col not in earnings_data_quart.columns.to_list()]
        not_in_finance_data = [col for col in finance_data_columns if col not in finance_data.columns.to_list()]


        for not_in in not_in_earnings_data:
            earnings_data[not_in] = np.nan

        for not_in in not_in_earnings_data_quart_columns:
            earnings_data_quart[not_in] = np.nan

        for not_in in not_in_finance_data:
            finance_data[not_in] = np.nan

        pattern = re.compile(r'(?<!^)(?=[A-Z])')

        earnings_data.rename(columns={col: pattern.sub('_', col.split('.')[0]).lower()
                                      for col in earnings_data.columns}, inplace=True)
        earnings_data_quart.rename(columns={col: pattern.sub('_', col.split('.')[0]).lower()
                                      for col in earnings_data_quart.columns}, inplace=True)
        finance_data.rename(columns={col: pattern.sub('_', col.split('.')[0]).lower()
                                      for col in finance_data.columns}, inplace=True)

        earnings_data['date'] = pd.to_datetime(earnings_data['date'], unit='s')
        earnings_data_quart['earningsDate'] = pd.to_datetime(earnings_data_quart['earningsDate'], unit='s')
        finance_data['date'] = pd.to_datetime(finance_data['date'], unit='s')

        return earnings_data, earnings_data_quart, finance_data

    def _organize_financials(self, data):
        financials = data['quoteSummary']['result'][0]['financialData']

        financials = json_normalize(financials)

        columns = ['currentPrice.raw', 'targetHighPrice.raw', 'targetLowPrice.raw', 'targetMeanPrice.raw',
                   'targetMedianPrice.raw', 'recommendationMean.raw', 'recommendationKey',
                   'numberOfAnalystOpinions.raw', 'totalCash.raw', 'totalCashPerShare.raw',
                   'returnOnAssets.raw', 'returnOnEquity.raw', 'grossProfits.raw',
                   'freeCashFlow.raw', 'operatingCashflow.raw', 'earningsGrowth.raw',
                   'revenueGrowth.raw', 'grossMargins.raw', 'ebitaMargins.raw', 'operatingMargins.raw',
                   'profitMargins.raw']

        not_ins = [col for col in columns if col not in financials.columns.to_list()]

        for not_in in not_ins:
            financials[not_in] = np.nan

        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        new_columns_dict = {col: pattern.sub('_', col.split('.')[0]).lower() for col in
                            financials.columns}

        financials.rename(columns=new_columns_dict, inplace=True)

        return financials
