import unittest

from quantpy.data.yahoo.YahooSummaryReader import YahooSummaryReader


class TestAllRequested(unittest.TestCase):

    def setUp(self):
        self.symbol = 'AAPL'
        self.summary_all = YahooSummaryReader(self.symbol, include_all=True).read()
        self.expected_got_str = 'Expected {} to not be None. Got type: {}'

    def test_properties_are_not_none(self):
        dont_check_properties = [prop for prop in dir(self.summary_all)
                                 if prop.startswith("__") or prop.startswith("_")]

        dont_check_properties += ['symbol', 'exception']

        for prop in dir(self.summary_all):
            if prop not in dont_check_properties:
                prop_value = getattr(self.summary_all, prop)

                if not callable(prop_value):
                    with self.subTest():
                        self.assertIsNotNone(prop_value, self.expected_got_str.format(prop, type(prop_value)))

    def test_exception_is_none(self):
        self.assertIsNone(self.summary_all.exception)


class TestNoneRequested(unittest.TestCase):

    def setUp(self):
        self.symbol = 'AAPL'
        self.reader = YahooSummaryReader(self.symbol, include_all=False)
        self.expected_got_str = 'Expected {} to be None. Got type: {}'

    def test_all_properties_none(self):
        with self.assertRaises(ValueError):
            self.reader.read()


class TestIndividualRequests(unittest.TestCase):

    def setUp(self):
        self.symbol = 'AAPL'
        self.expected_got_str = 'Expected {} to be None. Got type: {}'

        temp_summary = YahooSummaryReader(self.symbol, include_all=True).read()
        self.dont_check_properties = [prop for prop in dir(temp_summary)
                                      if prop.startswith("__") or prop.startswith("_")]
        self.dont_check_properties += ['symbol', 'exception', 'SummaryObject']

    def check_none_values(self, summary, extra_dont_check_properties):
        all_dont_check_properties = self.dont_check_properties + extra_dont_check_properties

        for prop in dir(summary):
            if prop not in all_dont_check_properties:
                with self.subTest():
                    with self.assertWarns(UserWarning):
                        prop_value = getattr(summary, prop)

                if not callable(prop_value):
                    with self.subTest():
                        self.assertIsNone(prop_value, self.expected_got_str.format(prop, type(prop_value)))

    def check_non_none_values(self, check_property, property_value):
        self.assertIsNotNone(property_value, self.expected_got_str.format(check_property, type(property_value)))

    def test_asset_profile_request(self):
        summary = YahooSummaryReader(self.symbol, include_asset_profile=True).read()
        check_property1 = 'profile'
        check_property2 = 'company_officers'

        self.check_none_values(summary, [check_property1, check_property2])

        self.check_non_none_values(check_property1, summary.profile)
        self.check_non_none_values(check_property2, summary.company_officers)

    def test_income_statement_history_request(self):
        summary = YahooSummaryReader(self.symbol, include_income_statement_history=True).read()
        check_property = 'income_statement_history'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.income_statement_history)

    def test_income_statement_history_quarterly_request(self):
        summary = YahooSummaryReader(self.symbol, include_income_statement_history_quarterly=True).read()
        check_property = 'income_statement_history_quarterly'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.income_statement_history_quarterly)

    def test_balance_sheet_history_request(self):
        summary = YahooSummaryReader(self.symbol, include_balance_sheet_history=True).read()
        check_property = 'balance_sheet_history'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.balance_sheet_history)

    def test_balance_sheet_history_quarterly_request(self):
        summary = YahooSummaryReader(self.symbol, include_balance_sheet_history_quarterly=True).read()
        check_property = 'balance_sheet_history_quarterly'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.balance_sheet_history_quarterly)

    def test_cash_flow_statement_history_request(self):
        summary = YahooSummaryReader(self.symbol, include_cash_flow_statement_history=True).read()
        check_property = 'cash_flow_statement_history'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.cash_flow_statement_history)

    def test_cash_flow_statement_history_quarterly_request(self):
        summary = YahooSummaryReader(self.symbol, include_cash_flow_statement_history_quarterly=True).read()
        check_property = 'cash_flow_statement_history_quarterly'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.cash_flow_statement_history_quarterly)

    def test_include_earnings_request(self):
        summary = YahooSummaryReader(self.symbol, include_earnings=True).read()
        check_property1 = 'earnings_estimates'
        check_property2 = 'earnings_estimates_quarterly'
        check_property3 = 'financials_quarterly'
        check_property4 = 'financials_yearly'

        self.check_none_values(summary, [check_property1, check_property2, check_property3, check_property4])

        self.check_non_none_values(check_property1, summary.earnings_estimates)
        self.check_non_none_values(check_property2, summary.earnings_estimates_quarterly)
        self.check_non_none_values(check_property3, summary.financials_quarterly)
        self.check_non_none_values(check_property4, summary.financials_yearly)

    def test_earnings_history_request(self):
        summary = YahooSummaryReader(self.symbol, include_earnings_history=True).read()
        check_property = 'earnings_history'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.earnings_history)

    def test_financial_data_request(self):
        summary = YahooSummaryReader(self.symbol, include_financial_data=True).read()
        check_property = 'financial_data'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.financial_data)

    def test_default_key_statistics_request(self):
        summary = YahooSummaryReader(self.symbol, include_default_key_statistics=True).read()
        check_property = 'default_key_statistics'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.default_key_statistics)

    def test_institution_ownership_request(self):
        summary = YahooSummaryReader(self.symbol, include_institution_ownership=True).read()
        check_property = 'institution_ownership'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.institution_ownership)

    def test_insider_holders_request(self):
        summary = YahooSummaryReader(self.symbol, include_insider_holders=True).read()
        check_property = 'insider_holders'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.insider_holders)

    def test_insider_transactions_request(self):
        summary = YahooSummaryReader(self.symbol, include_insider_transactions=True).read()
        check_property = 'insider_transactions'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.insider_transactions)

    def test_fund_ownership_request(self):
        summary = YahooSummaryReader(self.symbol, include_fund_ownership=True).read()
        check_property = 'fund_ownership'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.fund_ownership)

    def test_major_direct_holders_request(self):
        summary = YahooSummaryReader(self.symbol, include_major_direct_holders=True).read()
        check_property = 'major_direct_holders'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.major_direct_holders)

    def test_major_direct_holders_breakdown_request(self):
        summary = YahooSummaryReader(self.symbol, include_major_direct_holders_breakdown=True).read()
        check_property = 'major_direct_holders_breakdown'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.major_direct_holders_breakdown)

    def test_recommendation_trend_request(self):
        summary = YahooSummaryReader(self.symbol, include_recommendation_trend=True).read()
        check_property = 'recommendation_trend'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.recommendation_trend)

    def test_earnings_trend_request(self):
        summary = YahooSummaryReader(self.symbol, include_earnings_trend=True).read()
        check_property = 'earnings_trend'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.earnings_trend)

    def test_industry_trend_request(self):
        summary = YahooSummaryReader(self.symbol, include_industry_trend=True).read()
        check_property = 'industry_trend'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.industry_trend)

    def test_index_trend_request(self):
        summary = YahooSummaryReader(self.symbol, include_index_trend=True).read()
        check_property1 = 'index_trend_info'
        check_property2 = 'index_trend_estimate'

        self.check_none_values(summary, [check_property1, check_property2])

        self.check_non_none_values(check_property1, summary.index_trend_info)
        self.check_non_none_values(check_property2, summary.index_trend_estimate)

    def test_sector_trend_request(self):
        summary = YahooSummaryReader(self.symbol, include_sector_trend=True).read()
        check_property = 'sector_trend'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.sector_trend)

    def test_calendar_events_request(self):
        summary = YahooSummaryReader(self.symbol, include_calendar_events=True).read()
        check_property1 = 'calendar_events_earnings'
        check_property2 = 'calendar_events_dividends'

        self.check_none_values(summary, [check_property1, check_property2])

        self.check_non_none_values(check_property1, summary.calendar_events_earnings)
        self.check_non_none_values(check_property2, summary.calendar_events_dividends)

    def test_sec_filings_request(self):
        summary = YahooSummaryReader(self.symbol, include_sec_filings=True).read()
        check_property = 'sec_filings'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.sec_filings)

    def test_upgrade_downgrade_history_request(self):
        summary = YahooSummaryReader(self.symbol, include_upgrade_downgrade_history=True).read()
        check_property = 'upgrade_downgrade_history'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.upgrade_downgrade_history)

    def test_net_share_purchase_activity_request(self):
        summary = YahooSummaryReader(self.symbol, include_net_share_purchase_activity=True).read()
        check_property = 'net_share_purchase_activity'

        self.check_none_values(summary, [check_property])

        self.check_non_none_values(check_property, summary.net_share_purchase_activity)


if __name__ == '__main__':
    unittest.main(verbosity=0)
