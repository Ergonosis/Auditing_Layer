def get_storage_backend():
    from src.storage.delta_client import get_storage_backend as _gsb
    return _gsb()


__all__ = ["get_storage_backend"]
