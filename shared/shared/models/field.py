class Field:
    """
    Field for API
    """

    def __init__(self, name, *, default=None, callback_on=None, callback=None):
        """
        :param name: the name of the field
        :param default: default value for the field
        :param callback: preprocessor for the field
        """
        self.name = name
        self.default = default
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
            if self.default:
                return self.default
            raise Exception(f"Required value {self.name} must be specified")
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
