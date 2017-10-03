class Error(Exception):

    """Base class for all Eskapade core exceptions."""


class UnknownSetting(Error):

    """The user requested an unknown setting."""
