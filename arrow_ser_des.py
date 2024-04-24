from typing import Union, Mapping, Any, Iterable
from quixstreams.models.serializers import Serializer, Deserializer, SerializationContext
from quixstreams.models.serializers.exceptions import SerializationError

import pyarrow as pa

class ArrowSerializer(Serializer):
    def __init__(self):
        super().__init__()

    def __call__(self, data_dict: Any, ctx: SerializationContext) -> Union[str, bytes]:
        # Convert the dictionary to a PyArrow Table
        fields = [pa.field(name, pa.array([value]).type) for name, value in data_dict.items()]
        schema = pa.schema(fields)
        table = pa.Table.from_arrays([pa.array([value]) for value in data_dict.values()], schema=schema)

        # Serialize the Table to an Arrow IPC format in-memory
        output_stream = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(output_stream, table.schema)
        writer.write_table(table)
        writer.close()

        # Get the in-memory bytes buffer
        arrow_bytes = output_stream.getvalue().to_pybytes()

        return arrow_bytes

class ArrowDeserializer(Deserializer):
    def __init__(self):
        super().__init__()

    def __call__(self, value: bytes, ctx: SerializationContext) -> Union[Iterable[Mapping], Mapping]:
        input_stream = pa.BufferReader(value)
        reader = pa.ipc.open_stream(input_stream)
        table = reader.read_all()

        # Since scalar values were wrapped in lists, extract the first item to revert back to scalar
        data_dict_reconstructed = {column_name: table.column(column_name).to_pylist()[0] for column_name in table.column_names}

        return data_dict_reconstructed