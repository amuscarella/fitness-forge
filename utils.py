#Imports
from consts import *

#Functions
def ensure_dir(path):
    '''
    Ensure a directory exists, create it if it doesn't exist.
    :param path: Path to the directory.
    :return: None
    '''
    path = os.path.expandvars(os.path.expanduser(path))
    os.makedirs(path, exist_ok=True)

def exists_dir(path):
    '''
    Check if a directory exists.
    :param path: Path to the directory.
    :return: True if the directory exists, False otherwise.
    '''
    return os.path.exists(path)

def unzip(zip_file, tgt_dir):
    '''
    Unzip a zip file to a target directory.
    :param zip_file: Path to the zip file.
    :param tgt_dir: Path to the target directory.
    :return: None
    :raises: ValueError if the zip file is not a zip file or the path is unsafe.
    '''
    if not zipfile.is_zipfile(zip_file):
        raise ValueError(f"Not a zip file: {zip_file}")
    ensure_dir(tgt_dir)
    with zipfile.ZipFile(zip_file, 'r') as zf:
        for member in zf.namelist():
            dest = os.path.realpath(os.path.join(tgt_dir, member))
            if not dest.startswith(os.path.realpath(tgt_dir) + os.sep):
                raise ValueError(f"Unsafe path in zip: {member}")
            zf.extract(member, tgt_dir)

#Lists
def load_list(path, prefix="data/", add_ext=True):
    '''
    Load a list from a file.
    :param path: Path to the file.
    :param prefix: Prefix to the path.
    :param add_ext: If True, add the extension to the path.
    :return: List
    '''
    if add_ext:
        path += '.txt'
    if not path.startswith(prefix):
        path = prefix + path
    with open(path, 'r') as f:
        return [x.strip() for x in f.readlines()]

def save_list(lst, path, prefix="data/", add_ext=True):
    '''
    Save a list to a file.
    :param lst: List to save.
    :param path: Path to the file.
    :param prefix: Prefix to the path.
    :param add_ext: If True, add the extension to the path.
    :return: None
    '''
    if add_ext:
        path += '.txt'
    if not path.startswith(prefix):
        path = prefix + path
    with open(path, 'w') as f:
        for item in lst:
            f.write(item + '\n')

#DataFrames 
def load_df(path: str, prefix: str = "data/", sep: str | None = None, **kwargs) -> pd.DataFrame:
    """
    Simple, robust CSV/TSV loader with smart sep detection.
    :param path: Path to the file.
    :param prefix: Prefix to the path.
    :param sep: Separator to use.
    :param kwargs: Additional arguments for the read_csv function.
    :return: DataFrame
    """
    full_path = Path(prefix) / path
    full_path = full_path.expanduser()

    if not full_path.exists():
        raise FileNotFoundError(f"Data file not found: {full_path}")

    if not full_path.is_file():
        raise ValueError(f"Path is not a file: {full_path}")

    if sep is None:
        ext = full_path.suffix.lower()
        sep_dict = {
            '.tsv': '\t',
            '.csv': ',',
        }
        sep = sep_dict.get(ext, ',')

    return pd.read_csv(full_path, sep=sep, **kwargs)

def save_df(df, path, prefix="data/", add_ext=True, **kwargs):
    '''
    Save a dataframe to a file.
    :param df: DataFrame to save.
    :param path: Path to the file.
    :param prefix: Prefix to the path.
    :param add_ext: If True, add the extension to the path.
    :param kwargs: Additional arguments for the to_csv function.
    :return: None
    '''
    ext = os.path.splitext(path)[1]
    sep = kwargs.get('sep', ',')
    if not ext and add_ext:
        if sep == ',':
            ext = '.csv'
        elif sep == '\t':
            ext = '.tsv'
        else:
            raise ValueError(f"Unsupported separator: {sep}")
        path += ext
    path = prefix + path
    if ext == '.csv':
        df.to_csv(path, **kwargs)