"""
Class Representing a Machine Learning
Model and its alternate representations
"""


class Model:
    """
    Machine Learning model retrieved from the database
    """

    def __init__(self):
        self.representations = {}
        self.metadata = {}

    def add_metadata(self, name: str, metadata):
        """
        Add metadata to the model
        :param name: metadata key
        :param metadata: metadata value
        """
        self.metadata[name] = metadata

    def get_metadata(self, name: str):
        """
        Get metadata from model
        :param name: metadata key
        :return: metadata value
        """
        return self.metadata.get(name)

    def add_representation(self, name: str, representation):
        """
        Add a representation for the specified model
        :param name: name of the representation to create
        :param representation: representation object
        """
        if name in self.representations:
            raise Exception(f"Cannot register duplicate representation {name}")

        self.representations[name] = representation

    def get_representation(self, name: str):
        """
        Retrieve the representation specified
        :param name: name of the representation to retrieve
        :return: the representation
        """
        return self.representations.get(name)
