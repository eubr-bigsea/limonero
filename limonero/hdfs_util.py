from pyarrow import fs, csv
import pyarrow as pa
import pyarrow.parquet as pq

def copy_merge(local: fs.HadoopFileSystem, source_dir: str, 
        target: str, filename: str, n_chunks: int):
    """ """
    import pdb; pdb.set_trace()
    with local.open_output_stream(target) as stream:
        for i in range(n_chunks):
            number = str(i + 1).rjust(9, '0')
            name = f'{source_dir.rstrip("/")}/{filename}.part{number}'
            with local.open_input_file(name) as in_file:
                stream.write(in_file.read())
        
    for i in range(n_chunks):
        number = str(i + 1).rjust(9, '0')
        name = f'{source_dir.rstrip("/")}/{filename}.part{number}'
        local.delete_file(name)
        
def write(local: fs.HadoopFileSystem, path: str, chunk: any):
    """ Write data """
    with local.open_output_stream(path) as stream:
        stream.write(chunk)

def mkdirs(local: fs.HadoopFileSystem, path: str) -> bool:
    """
    Create directory, including any parent.
    """
    local.create_dir(path)

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

def _csv_options(ds):
    convert_options = csv.ConvertOptions(
        null_values=(ds.treat_as_missing or '').split(','),
    )
    parse_options = csv.ParseOptions(
        delimiter=ds.attribute_delimiter,
        quote_char=ds.text_delimiter,
        escape_char=None,
        newlines_in_values=ds.is_multiline,
        invalid_row_handler=lambda row: 'skip',
    )
    read_options = csv.ReadOptions(
        column_names=[attr.name for attr in ds.attributes],
        encoding=ds.encoding,
        skip_rows=0 if len(ds.attributes) == 0 else 1
    )
    return convert_options, parse_options, read_options

def sample_csv(local: fs.HadoopFileSystem, path: str, size: int, ds):
    """ Return a sample of size rows from CSV file """
    lines = []
    convert_options, parse_options, read_options = _csv_options(ds)

    with local.open_input_file(path) as f:
        with csv.open_csv(f, convert_options=convert_options,
                read_options=read_options,
                parse_options=parse_options) as reader:
            for next_chunk in reader:
                next_table = pa.Table.from_batches([next_chunk])
                next_table = next_table.slice(0, size - len(lines))
                lines.extend(next_table.to_pylist())
                if len(lines) >= size:
                    break

    return lines[:size]
def infer_csv(local: fs.HadoopFileSystem, path: str, size: int, ds):
    """ Infer attributes from CSV file """
    convert_options, parse_options, read_options = _csv_options(ds)

    with local.open_input_file(path) as f:
        with csv.open_csv(f, convert_options=convert_options,
                read_options=read_options,
                parse_options=parse_options) as reader:
            for next_chunk in reader:
                next_table = pa.Table.from_batches([next_chunk])
                break

    schema = next_table.schema
    return zip(schema.names, schema.types)
