from quantpy.data.yahoo.YahooQuoteReader import YahooQuoteReader
import inspect


def main():
    quote = YahooQuoteReader('AAPL', period='max').read()

if __name__=='__main__':
    main()