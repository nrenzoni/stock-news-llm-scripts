import zstandard as zstd
import numpy as np
import io
import json

import grpc
from . import embedding_pb2
from . import embedding_pb2_grpc

compressor = zstd.ZstdCompressor()
decompressor = zstd.ZstdDecompressor()


def bytes_as_np_ndarray(bytes_data):
    memory_file = io.BytesIO(bytes_data)
    return np.load(memory_file)


class GrpcEmbeddingClient:
    def __init__(self, host):
        self.host = host
        self.port = str(50051)

    def calc_embeddings(self, texts: list[str]):
        assert len(texts) <= 1_000, 'Too many texts to embed'

        channel = grpc.insecure_channel(f'{self.host}:{self.port}')
        stub = embedding_pb2_grpc.EmbeddingServiceStub(channel)

        # Example data to send
        texts_bytes = bytes(
            json.dumps(texts),
            'utf-8'
        )
        texts_bytes_compressed = compressor.compress(texts_bytes)

        # Create a request
        request = embedding_pb2.EmbeddingRequest(
            embeddingsListBinary=texts_bytes_compressed
        )

        # Call the CalculateEmbeddings method
        response = stub.CalculateEmbeddings(request)
        embeddings_bytes_compressed = response.embeddingsListBinary
        embeddings_bytes = decompressor.decompress(embeddings_bytes_compressed)
        embeddings = bytes_as_np_ndarray(embeddings_bytes)

        return embeddings
