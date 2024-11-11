from .api import Request, Response

class MetadataResponse_v0(Response):
    pass


class MetadataResponse_v1(Response):
    pass


class MetadataResponse_v2(Response):
    pass


class MetadataResponse_v3(Response):
    pass


class MetadataResponse_v4(Response):
    pass


class MetadataResponse_v5(Response):
    pass


class MetadataRequest_v0(Request):
    pass


class MetadataRequest_v1(Request):
    pass


class MetadataRequest_v2(Request):
    pass


class MetadataRequest_v3(Request):
    pass


class MetadataRequest_v4(Request):
    pass


class MetadataRequest_v5(Request):
    """
    The v5 metadata request is the same as v4.
    An additional field for offline_replicas has been added to the v5 metadata response
    """
    pass


MetadataRequest = ...
MetadataResponse = ...
