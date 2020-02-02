from quantpy.data.yahoo.YahooSummaryReader import YahooSummaryReader
from quantpy.data.yahoo.YahooQuoteReader import YahooQuoteReader


def get_yahoo_quote(symbols, start=None, end=None, interval='1d', events=False, pre_post=True):
    return YahooQuoteReader(symbols, start, end, interval, events, pre_post).read()


def get_yahoo_summary(symbols):
    return YahooSummaryReader(symbols).read()


def get_yahoo_profile(symbols):
    return YahooSummaryReader(symbols, include_asset_profile=True).read()


def get_yahoo_income_statement(symbols, include_quarterly=True, include_historical=False):
    return YahooSummaryReader(symbols,
                              include_income_statement_history=include_historical,
                              include_income_statement_history_quarterly=include_quarterly).read()


def get_yahoo_balance_sheet(symbols, include_quarterly=True, include_historical=False):
    return YahooSummaryReader(symbols,
                              include_balance_sheet_history=include_historical,
                              include_balance_sheet_history_quarterly=include_quarterly).read()


def get_yahoo_cash_flow_statement(symbols, include_quarterly=True, include_historical=False):
    return YahooSummaryReader(symbols,
                              include_cash_flow_statement_history=include_historical,
                              include_cash_flow_statement_history_quarterly=include_quarterly).read()


def get_yahoo_earnings(symbols, include_historical=False, include_trend=False):
    return YahooSummaryReader(symbols,
                              include_earnings=True,
                              include_earnings_history=include_historical,
                              include_earnings_trend=include_trend).read()


def get_yahoo_financial(symbols):
    return YahooSummaryReader(symbols, include_financial_data=True).read()


def get_yahoo_key_statistics(symbols):
    return YahooSummaryReader(symbols, include_default_key_statistics=True).read()


def get_yahoo_ownership(symbols, include_funds=True, include_institutions=True):
    return YahooSummaryReader(symbols,
                              include_fund_ownership=include_funds,
                              include_institution_ownership=include_institutions).read()


def get_yahoo_insiders(symbols, include_insiders=True, include_insider_transactions=True):
    return YahooSummaryReader(symbols,
                              include_insider_holders=include_insiders,
                              include_insider_transactions=include_insider_transactions).read()


def get_yahoo_major_holders(symbols, include_major_holders=True, include_breakdown=False):
    return YahooSummaryReader(symbols,
                              include_major_direct_holders=include_major_holders,
                              include_major_direct_holders_breakdown=include_breakdown).read()


def get_yahoo_trends(symbols, include_recommendation=True, include_industry=False,
                     include_index=False, include_sector=False):
    return YahooSummaryReader(symbols,
                              include_recommendation_trend=include_recommendation,
                              include_industry_trend=include_industry,
                              include_index_trend=include_index,
                              include_sector_trend=include_sector).read()


def get_yahoo_calendar(symbols):
    return YahooSummaryReader(symbols, include_calendar_events=True).read()


def get_yahoo_sec_filings(symbols):
    return YahooSummaryReader(symbols, include_sec_filings=True).read()


def get_yahoo_upgrades_downgrades(symbols):
    return YahooSummaryReader(symbols, include_upgrade_downgrade_history=True).read()


def get_yahoo_share_purchase_activity(symbols):
    return YahooSummaryReader(symbols, include_net_share_purchase_activity=True).read()
