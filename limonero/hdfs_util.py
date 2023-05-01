from pyarrow import fs
import pyarrow.parquet as pq

def exists(local: fs.HadoopFileSystem, path: str) -> bool:
    """
    Test if a path exists in a HDFS server.
    """
    return local.get_file_info(path).type != fs.FileType.NotFound

def sample_parquet(local: fs.HadoopFileSystem, path: str, size: int):
    """ Return a sample of size rows from Parquet file """
    ds = pq.ParquetDataset(path, filesystem=local)
    return ds.read().slice(0, size).to_pylist()

def infer_parquet(local: fs.HadoopFileSystem, path: str):
    """ Return a sample of size rows from Parquet file """
    ds = pq.ParquetDataset(path, filesystem=local)
    schema = ds.read().schema
    return zip(schema.names, schema.types)


