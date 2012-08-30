
"""
    Functionality shared across fileman modules
"""

class FilemanError(Exception):
    def __init__(self, message, *args, **kwargs):
        self._message = message
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return "FilemanError: %s" % self.message()

    def __unicode__(self):
        return "FilemanError: %s" % self.message()

    def message(self):
        rv = [self._message]
        if self.args:
            rv.append(unicode(self.args))
        if self.kwargs:
            rv.append(unicode(self.kwargs))
        return '\n'.join(rv)

class FilemanErrorNumber(FilemanError):
    """Fileman returned an error with information"""
    def __init__(self, dierr=None, codes=None, texts=None):
        """Create an error from the dierr structure"""
        if codes:
            self.codes = codes
        else:
            self.codes = []
        if texts:
            self.texts = texts
        else:
            self.texts = []
        if dierr:
            for errnum, code in dierr['DIERR'].keys_with_decendants():
                if errnum == 'E':
                    continue
                self.codes.append(code)
                text = []
                for key in dierr['DIERR'][errnum]['TEXT'].keys():
                    text.append(dierr['DIERR'][errnum]['TEXT'][key].value) 
                self.texts.append('\n'.join(text))

    def message(self):
        rv = ["FilemanErrorNumber:"]
        for c, t in zip(self.codes, self.texts):
            rv.append("   %s: %s" % (c, t))
        return '\n'.join(rv)

class ROWID:
    """Type describing the ROWID column"""

class STRING:
    """Type describing the ROWID column"""
