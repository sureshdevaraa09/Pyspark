from pyspark.sql.functions import col, lit, concat, concat_ws, sha1, encode
from pyspark.sql import DataFrame
import uuid

class UUIDHash:
    def get_ns(self, namespace: str = None) -> list:
        """
        This function generates a namespace UUID.

        Parameters:
        namespace (str, optional): The namespace for the UUIDs hash. Defaults to None.

        Returns:
        Column: A column that contains binary data, which is the namespace UUID.
        """
        # If the namespace is already a UUID, return it
        if isinstance(namespace, uuid.UUID):
            return namespace
        # If the namespace is not provided, use 'nike.com' as the default namespace
        ns = 'nike.com' if not namespace else namespace.strip()
        # Generate a UUID using the namespace
        ns = uuid.uuid5(uuid.NAMESPACE_DNS, ns)
        # Return the namespace UUID as a binary column
        return list(ns.bytes)

    def sort(self, col_list: list, sort_order: bool) -> list:
        """
        This function sorts a list of strings.

        Parameters:
        col (list): The list of strings to be sorted.
        sort_order (bool): The order in which to sort the strings.
                           If True, the strings are sorted in descending order.
                           If False, the strings are sorted in ascending order.

        Returns:
        list: The sorted list of strings.
        """
        # Strip leading and trailing whitespaces from each string in the list
        col_list = [x.strip() for x in col_list]
        # Sort the list of strings based on the sort_order
        col_list = sorted(col_list, reverse=sort_order)
        return col_list

    def uuidSHA1(self, df: DataFrame, source_columns: list, target_column: str, delimiter: str = '|', namespace: str = None, extra_string: str = "", sort_source_col: bool = True) -> DataFrame:
        """
        This function generates a UUID5 hash for a given DataFrame and adds it as a new column.

        Parameters:
        df (DataFrame): The input DataFrame.
        source_columns (list): The list of column names to be used for generating the hash.
        target_column (str): The name of the new column to be added to the DataFrame.
        delimiter (str, optional): The delimiter used to concatenate the source columns. Defaults to "|".
        namespace (str, optional): The namespace for the UUIDs hash. Defaults to None.
        extra_string (str, optional): An extra string to be added to the beginning of the hash. Defaults to "".
        sort_source_col (bool, optional): A flag to indicate whether to sort the source columns. Defaults to True.

        Returns:
        DataFrame: The DataFrame with the new column added.
        """
        # Get the namespace UUID
        ns = self.get_ns(namespace)
        # Check if source_columns is a list and sort it if required
        if isinstance(source_columns, list):
            list_reverse = not sort_source_col
            source_columns = self.sort(source_columns, list_reverse)
        else:
            raise ValueError("Expected input source column type is list, but given value is in different datatype")

        # Concatenate the source columns with the delimiter
        cols_to_hash = concat_ws(delimiter, *source_columns)
        # Add the extra string to the beginning of the hash
        cols_to_hash = concat(lit(extra_string), cols_to_hash)
        # Encode the hash to bytes using UTF-8 encoding
        cols_to_hash = encode(cols_to_hash, 'UTF-8')
        # Add the namespace UUID to the beginning of the hash
        cols_to_hash = concat(lit(ns), cols_to_hash)
        # Calculate the SHA1 hash of the hash
        source_columns_sha1 = sha1(cols_to_hash)
        # Extract a part of the SHA1 hash and modify it to form a variant part of the UUID
        variant_part = source_columns_sha1.substr(17, 4)
        variant_part = variant_part.cast("binary")
        variant_part = variant_part.cast("string")
        variant_part = lit('8') + variant_part.substr(2, 3)
        variant_part = lit('89ab') + variant_part.substr(3, 2)
        variant_part = variant_part.cast("int").cast("string")
        # Construct the UUID using parts of the SHA1 hash and the variant part
        target_col_uuid = concat_ws(
            '-',
            source_columns_sha1.substr(1, 8),
            source_columns_sha1.substr(9, 4),
            lit('4') + source_columns_sha1.substr(14, 3),  # Set version
            variant_part,
            source_columns_sha1.substr(21, 12),
        )
        # Add the new column to the DataFrame and return it
        return df.withColumn(target_column, target_col_uuid)
