from quantpy.data.yahoo.YahooSummaryReader import YahooSummaryReader

def main():
    yqr = YahooSummaryReader('AAPL').read()
    x = 1

if __name__=='__main__':
    main()