from quantpy.data.yahoo.YahooSummaryReader import YahooSummaryReader
import inspect


def main():
    summary = YahooSummaryReader('AAPL').read()

    for prop in dir(summary):
        if(prop[:2] != "__"):
            print(prop, getattr(summary, prop).value)



    ysr = YahooSummaryReader(*['AAPL', False])

    print(1)

if __name__=='__main__':
    main()