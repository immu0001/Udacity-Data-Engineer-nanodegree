
import os
import glob
import pandas as pd


class FileMgr:
    """
    Manage I/O files and directories and transform files to pandas dataframe and vice versa.
    """

    @staticmethod
    def get_directory_files_list(directory_path: str):
        """
        Get all the files in a directory path.
        :directory_path: path/to/directory.
        :return: array that contains all the paths to the files.
        """
        file_path_list = []

        for root, dirs, files in os.walk(directory_path):
            file_path_list = glob.glob(os.path.join(root, '*'))

        return file_path_list

    @staticmethod
    def file_num_rows(file_path: str) -> int:
        """
        Count the number of rows of a file.
        :param file_path: path/to/file.
        :return: number of lines of the file.
        """
        with open(file_path, 'r', encoding='utf8') as f:
            return sum(1 for line in f)

    @staticmethod
    def read_file_to_pd(file_path: str, schema):
        """
        Read a file into a pandas dataframe with a determined schema.
        :param file_path: path/to/file.
        :param schema: schema of the pandas dataframe
        :return: pandas dataframe
        """
        df = pd.read_csv(file_path)

        for k, v in schema.items():
            df[k] = df[k].astype(v)

        return df

    @staticmethod
    def files_to_pd(file_path_list):
        """
        Read all the files of an array and makes append of all in only one pandas dataframe.
        :param file_path_list: array of files
        :return: pandas dataframe with all the files.
        """

        li = []

        for filename in file_path_list:
            df = pd.read_csv(filename, index_col=None, header=0)
            li.append(df)

        return pd.concat(li, axis=0, ignore_index=True)

    @staticmethod
    def pd_to_file(file_path, df):
        """
        Writes a pandas dataframe in a csv file.
        :param file_path: path/to/csv/file-
        :param df: pandas dataframe.
        """
        df.to_csv(file_path, sep=',', encoding='utf-8', header=1, index=False)
