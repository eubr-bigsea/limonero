from pyarrow import fs, csv, types as pa_types
import pyarrow as pa
import pyarrow.parquet as pq
import io
from gettext import gettext

def copy_merge(local: fs.HadoopFileSystem, source_dir: str, 
        target: str, filename: str, n_chunks: int):
    """ """
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

def is_directory(local: fs.HadoopFileSystem, path: str) -> bool:
    """
    Test if a path is a directory in a HDFS server.
    """
    return local.get_file_info(path).type == fs.FileType.Directory

def exists(local: fs.HadoopFileSystem, path: str) -> bool:
    """
    Test if a path exists in a HDFS server.
    """
    return local.get_file_info(path).type != fs.FileType.NotFound

def sample_parquet(local: fs.HadoopFileSystem, path: str, size: int, schema=None):
    """ Return a sample of size rows from Parquet file """
    if schema:
        ds = pq.ParquetDataset(path, filesystem=local, schema=schema)
    else:
        ds = pq.ParquetDataset(path, filesystem=local)
    return ds.read().slice(0, size).to_pylist()

def get_parquet_schema(ds):
    """ Return a sample of size rows from Parquet file """
    new_attrs = []
    tests = [
         (pa.string, 'CHARACTER'),
         (pa.int32, 'INTEGER'),
         (pa.int64, 'LONG'),
         (pa.float64, 'DOUBLE'),
         (pa.float32, 'FLOAT'),
         (lambda: pa.timestamp('s'), 'DATETIME'),
         (pa.date64, 'DATE'),
         (pa.binary, 'BINARY'),
         (lambda: pa.decimal128(10), 'DECIMAL'),
         (pa.large_string, 'TEXT'),
         (pa.time64, 'TIME'),
         (lambda: pa.list(pa.string), 'VECTOR'),
    ]

    for attr in ds.attributes:
        for dtype, limonero_type in tests:
            ok = False
            if attr.type == limonero_type:
                new_attrs.append((attr.name, dtype()))
                ok = True
                break
        if not ok:
            new_attrs.append((attr.name, pa.string()))
    return pa.schema(new_attrs)

def infer_parquet(local: fs.FileSystem, path: str):
    """ Return a sample of size rows from Parquet file """
    ds = pq.ParquetDataset(path, filesystem=local)
    schema = ds.read().schema
    tests = [
         (pa_types.is_unicode, 'CHARACTER'),
         (pa_types.is_boolean, 'INTEGER'),
         (pa_types.is_integer, 'INTEGER'),
         (pa_types.is_float64, 'DOUBLE'),
         (pa_types.is_floating, 'FLOAT'),
         (pa_types.is_timestamp, 'DATETIME'),
         (pa_types.is_date, 'DATE'),
         (pa_types.is_binary, 'BINARY'),
         (pa_types.is_decimal, 'DECIMAL'),
         (pa_types.is_list, 'VECTOR'),
         (pa_types.is_large_string, 'TEXT'),
    ]

    limonero_types = []
    for t in schema.types:
        ok = False
        for test, dtype in tests:
            ok = test(t)
            if ok:
                limonero_types.append(dtype)
                break  
        if not ok:
            raise ValueError(gettext(
                'Unsupported Parquet data type: {t}').format( 
                t = str(t))) 
            
    return zip(schema.names, limonero_types)

def _csv_options(ds):
    convert_options = csv.ConvertOptions(
        null_values=(ds.treat_as_missing or '').split(','),
    )
    parse_options = csv.ParseOptions(
        delimiter=ds.attribute_delimiter or ',',
        quote_char=ds.text_delimiter or '"',
        newlines_in_values=bool(ds.is_multiline),
        invalid_row_handler=lambda row: 'skip',
    )
    read_options = csv.ReadOptions(
        column_names=[attr.name for attr in ds.attributes],
        encoding=ds.encoding or 'utf8',
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

def download_parquet1(local: fs.HadoopFileSystem, path: str):
    """ """
    ds = pq.ParquetDataset(path, filesystem=local).read()
    with open('/tmp/titan.parquet', 'wb') as lixo: 
        with pq.ParquetWriter(lixo, ds.schema, store_schema=True) as writer:
            for i, batch in enumerate(ds.to_batches(max_chunksize=1500)):
                writer.write_batch(batch)

                # with pq.ParquetWriter(sink, batch.schema, store_schema=i==0) as writer:
                #    writer.write_batch(batch)
                # pq.write_table(pa.Table.from_batches([batch]), sink,
                #    store_schema=False)
                #buf = sink.getvalue()
                # sink.seek(0)
                #data = buf.to_pybytes()
                #lixo.write(data)
    yield 'data'
def download_parquet3(local: fs.HadoopFileSystem, path: str):
    """ """
    ds = pq.ParquetDataset(path, filesystem=local).read()
    batches = ds.to_batches(max_chunksize=10200)
    BUFFER_SIZE = 8192
    with open('/tmp/titan.parquet', 'wb') as lixo: 
        # sink = pa.BufferOutputStream()
        sink = io.BytesIO()
        writer = pq.ParquetWriter(sink, ds.schema, store_schema=True)
        for i, batch in enumerate(batches):
            #print('Batch n####', i, sink.getbuffer().nbytes)
            writer.write_batch(batch)
            #print('After Batch n####', i, sink.getbuffer().nbytes)

            # with pq.ParquetWriter(sink, batch.schema, store_schema=i==0) as writer:
            #    writer.write_batch(batch)
            # pq.write_table(pa.Table.from_batches([batch]), sink,
            #    store_schema=False)
            done = False
            # data = sink.getbuffer()
            print('*' * 10, 'Batch ', i)
            sink.seek(0)
            lixo.write(sink.getbuffer())
            # while not done:
            #     data = sink.read(BUFFER_SIZE)
            #     amount = len(data)
            #     if amount > 0:
            #         lixo.write(data)
            #         print('#' * 20, amount, i)
            #     else:
            #         done = True
            # data = buf.to_pybytes()
            sink.truncate()
            sink.seek(0)
            # yield  data
        writer.close()
        final = sink.getbuffer()
        print('>>>>>>>>>', final)
        lixo.write(final)
    yield 'ok'

def download_parquet(local: fs.HadoopFileSystem, path: str):
    """ """
    ds = pq.ParquetDataset(path, filesystem=local).read()
    batches = ds.to_batches(max_chunksize=10_000)

    sink = io.BytesIO()
    writer = pq.ParquetWriter(sink, ds.schema, store_schema=True)
    for i, batch in enumerate(batches):
        writer.write_batch(batch)
        sink.seek(0)
        data = sink.getbuffer().tobytes()
        sink.truncate()
        sink.seek(0)
        yield data

    writer.close()
    yield sink.getbuffer().tobytes()

def download_parquet_as_csv(local: fs.HadoopFileSystem, path: str):
    ds = pq.ParquetDataset(path, filesystem=local).read()
    BUFFER_SIZE = 4096

    buf = io.BytesIO()
    for i, batch in enumerate(ds.to_batches(max_chunksize=1000)):
        csv.write_csv(batch, buf, csv.WriteOptions(include_header=(i==0)))
        buf.seek(0) # for reading
        done = False
        while not done:
            data = buf.read(BUFFER_SIZE)
            amount = len(data)
            if amount != BUFFER_SIZE:
                done = True
            yield bytes(data)
        buf.truncate(0) 
        buf.seek(0) # for writing

def download_file(local: fs.HadoopFileSystem, path: str):
    BUFFER_SIZE = 4096
    done = False
    with local.open_input_file(path) as stream:
        while not done:
            data = stream.read(BUFFER_SIZE)
            amount = len(data)
            if amount != BUFFER_SIZE:
                done = True
            yield bytes(data)


def get_parsed_uri(parsed, include_port=True):
    if parsed.port and include_port:
        str_uri = f'{parsed.scheme}://{parsed.hostname}:{parsed.port}'
    else:
        str_uri = f'{parsed.scheme}://{parsed.hostname}'
    return str_uri
