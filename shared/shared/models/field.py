class Field:
    """
    Field for API
    """

    def __init__(self, name, *, default=None, use_default=False, callback_on=None, callback=None):
        """
        :param name: the name of the field
        :param default: default value for the field
        :param callback: preprocessor for the field
        :param callback_on: the type to run the callback on
        :param use_default: whether or not to use the default
        """
        self.name = name
        self.default = default
        # We need use_default (although it is repetitive) because None and non specified
        # defaults look the same as not specifying them at all.
        self.use_default = use_default
        self.callback_on = callback_on
        self.callback = callback

    def process(self, value):
        """
        Preprocess field with callback
        :param value: the value to process
        :return: preprocessed value
        """
        if not self.callback:
            return value

        if self.callback_on:
            if isinstance(value, self.callback_on):
                return self.callback(value)
            else:
                return value
        else:
            return self.callback(value)

    def get_value(self, value):
        """
        Get the processed value of a field
        :param value: given value
        :return: processed value
        """
        if value is None:
            if self.use_default:
                return self.default
            raise KeyError(self.name)
        else:
            return self.process(value)

    @staticmethod
    def string_to_boolean_converter(value):
        """
        Reusable Callback to convert strings to booleans from
        HTML
        :param value: value to convert
        :return: converted value
        """
        return True if value == "true" else False if value == "false" else None
