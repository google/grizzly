class ParsingError(Exception): pass
class UnknownScenario(ParsingError): pass
class UnsupportedTypeError(ParsingError): pass
class SerializingParamsNotReady(ParsingError): pass
class ParsingLookupError(ParsingError): pass
class ColumnLookupError(ParsingLookupError): pass
class TableLookupError(ParsingLookupError): pass
class LoadingError(ParsingError): pass
