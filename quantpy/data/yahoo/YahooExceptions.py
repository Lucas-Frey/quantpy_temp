
class YahooError(Exception):
    pass


class YahooRuntimeError(YahooError):
    pass


class YahooRequestError(YahooError):
    pass


class YahooAllModulesNotFoundError(YahooError):
    pass


class YahooModuleNotFoundError(YahooError):
    pass


class YahooModuleFormatError(YahooError):
    pass

